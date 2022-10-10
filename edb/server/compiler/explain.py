#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations
from typing import *

import dataclasses
import json
import re
import uuid
import struct

# import immutables

# from edb import errors

# from edb.server import defines
# from edb.pgsql import compiler as pg_compiler

# from edb import edgeql
from edb.common import ast
from edb.common import debug
# from edb.common import verutils
# from edb.common import uuidgen
# from edb.common import ast

from edb.edgeql import ast as qlast
# from edb.edgeql import codegen as qlcodegen
# from edb.edgeql import compiler as qlcompiler
# from edb.edgeql import qltypes
# from edb.edgeql import quote as qlquote

# from edb.ir import staeval as ireval
from edb.ir import ast as irast
from edb.ir import utils as irutils

# from edb.schema import database as s_db
# from edb.schema import ddl as s_ddl
# from edb.schema import delta as s_delta
# from edb.schema import functions as s_func
# from edb.schema import links as s_links
# from edb.schema import properties as s_props
# from edb.schema import modules as s_mod
# from edb.schema import name as s_name
# from edb.schema import objects as s_obj
# from edb.schema import objtypes as s_objtypes
from edb.schema import constraints as s_constr
from edb.schema import pointers as s_pointers
# from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
# from edb.schema import types as s_types
# from edb.schema import utils as s_utils

from edb.pgsql import ast as pgast
from edb.pgsql.compiler import pathctx
# from edb.pgsql import common as pg_common
# from edb.pgsql import delta as pg_delta
# from edb.pgsql import dbops as pg_dbops
# from edb.pgsql import params as pg_params
# from edb.pgsql import types as pg_types

# from edb.server import config

# from . import dbstate
# from . import enums
# from . import sertypes
# from . import status

uuid_core = '[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}'
uuid_re = re.compile(
    f'(\.?"?({uuid_core})"?)',
    re.I
)


@dataclasses.dataclass
class AnalysisInfo:
    aliases: dict[str, pgast.PathRangeVar]
    alias_to_path_id: dict[str, tuple[irast.PathId, Optional[int]]]
    sets: dict[irast.PathId, set[irast.Set]]


def analyze_queries(
    ql: qlast.Base, ir: irast.Statement, pg: pgast.Base,
    *, schema: s_schema.Schema,
) -> AnalysisInfo:
    rvars = ast.find_children(pg, pgast.PathRangeVar)
    queries = ast.find_children(pg, pgast.Query)

    # Map subqueries back to their rvars
    subq_to_rvar: dict[pgast.Query, pgast.RangeSubselect] = {}
    for rvar in rvars:
        if isinstance(rvar, pgast.RangeSubselect):
            assert rvar.subquery not in subq_to_rvar
            subq_to_rvar[rvar.subquery] = rvar

    # Find all *references* to an rvar in path_rvar_maps
    reverse_path_rvar_map: dict[
        pgast.PathRangeVar,
        list[tuple[tuple[irast.PathId, str], pgast.Query]]
    ] = {}
    for qry in queries:
        for key, rvar in qry.path_rvar_map.items():
            reverse_path_rvar_map.setdefault(rvar, []).append((key, qry))

    # Map aliases to rvars and then to path ids
    aliases = {
        rvar.alias.aliasname: rvar for rvar in rvars if rvar.alias.aliasname
    }
    path_ids = {
        alias: (rvar.relation.path_id, rvar.relation.path_scope_id)
        for alias, rvar in aliases.items()
        if isinstance(rvar, pgast.RelRangeVar)
        and isinstance(rvar.relation, pgast.BaseRelation)
        and rvar.relation.path_id
    }

    # Find all the sets
    sets: dict[irast.PathId, set[irast.Set]] = {}
    for s in ast.find_children(ir, irast.Set):
        if s.context:
            sets.setdefault(s.path_id, set()).add(s)

    scopes = irutils.find_path_scopes(ir)

    # Trying to produce good contexts for stuff
    # KEY FACT: We often duplicate code for with bindings
    for alias, (path_id, scope_id) in path_ids.items():
        if scope_id is None:  # ???
            continue
        for s in sets.get(path_id, ()):
            if scopes.get(s) == scope_id and s.context:
                asets = [s]

                # Loop back through...
                cpath = path_id
                rvar = aliases[alias]
                while True:
                    sources = [
                        s for k, s in
                        reverse_path_rvar_map.get(rvar, ())
                        if k == (cpath, 'source')
                    ]
                    print(sources, cpath)
                    if sources:
                        source = sources[0]
                        cpath = pathctx.reverse_map_path_id(
                            cpath, source.view_path_id_map)

                        ns = tuple(sets.get(cpath, ()))
                        if len(ns) == 1 and ns[0].context:
                            if ns[0] not in asets:
                                asets.append(ns[0])

                        if source not in subq_to_rvar:
                            break
                        rvar = subq_to_rvar[source]
                    else:
                        break

                print(
                    alias, [
                        (x.context.start, x.context.end)
                        for x in asets if x.context
                    ]
                )
                print(asets)
                for x in asets:
                    debug.dump(x.context)

    return AnalysisInfo(
        aliases=aliases,
        alias_to_path_id=path_ids,
        sets=sets,
    )

# This is just really hokey string replacement stuff...
# We need to think about whether we can do better and how we can
# represent it.
def json_fixup(
    obj: Any, schema: s_schema.Schema, idx: int | str | None = None
) -> Any:
    if isinstance(obj, list):
        return [json_fixup(x, schema) for x in obj]
    elif isinstance(obj, dict):
        return {
            k: json_fixup(v, schema, k) for k, v in obj.items()
            if k not in ('Schema',)
        }
    elif isinstance(obj, str):
        if idx == 'Index Name':
            obj = obj.replace('_source_target_key', ' forward link index')
            obj = obj.replace(';schemaconstr', ' exclusive constraint index')
            obj = obj.replace('_target_key', ' backward link index')
            # ???
            obj = obj.replace('_index', ' index')
            # obj = obj.replace('_index', ' backward inline link index')

        for (full, m) in uuid_re.findall(obj):
            uid = uuid.UUID(m)
            sobj = schema.get_by_id(uid, default=None)
            if sobj:
                dotted = full[0] == '.'
                if isinstance(sobj, s_pointers.Pointer):
                    # If a pointer is on the RHS of a dot, just use
                    # the short name. But otherwise, grab the source
                    # and link it up
                    s = str(sobj.get_shortname(schema).name)
                    if sobj.is_link_property(schema):
                        s = f'@{s}'
                    if not dotted:
                        src_name = sobj.get_source(schema).get_name(schema)
                        s = f'{src_name}.{s}'
                elif isinstance(sobj, s_constr.Constraint):
                    s = sobj.get_verbosename(schema, with_parent=True)
                else:
                    s = str(sobj.get_name(schema))

                if dotted:
                    s = '.' + s
                obj = uuid_re.sub(s, obj, count=1)

        return obj
    else:
        return obj


def analyze_explain_output(
    dbv: Any, # dbview.DatabaseConnectionView,
    compiled: Any, #dbview.CompiledQuery,
    data: list[list[bytes]],
) -> bytes:
    debug.header('Explain')

    unit = compiled.query_unit_group[0]
    ql: qlast.Base
    ir: irast.Statement
    pg: pgast.Base
    ql, ir, pg = unit.query_asts
    schema = ir.schema
    if not (len(data) == 1 and len(data[0]) == 1):
        breakpoint()
    assert len(data) == 1 and len(data[0]) == 1
    # print('DATA', data)
    try:
        plan = json.loads(data[0][0])
    except UnicodeDecodeError:
        breakpoint()

    plan = json_fixup(plan, schema)

    debug.dump(plan)

    # XXX: OBVIOUSLY EARLY
    info = analyze_queries(ql, ir, pg, schema=schema)
    # debug.dump(info, _ast_include_meta=False)

    return make_message([{
        'edgeql': "UNIMPLEMENTED",
        'sql': plan,
    }])


def make_message(obj: Any) -> bytes:
    omsg = json.dumps(obj).encode('utf-8')
    msg = struct.pack(
        "!hic",
        1,
        len(omsg) + 1,
        # XXX: why isn't it b'\x01'??
        b' ',
    ) + omsg
    return msg
