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

# import collections
# import dataclasses
import json
# import hashlib
# import pickle
# import textwrap
# import uuid
import struct

# import immutables

# from edb import errors

# from edb.server import defines
# from edb.pgsql import compiler as pg_compiler

# from edb import edgeql
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
# from edb.schema import pointers as s_pointers
# from edb.schema import reflection as s_refl
# from edb.schema import schema as s_schema
# from edb.schema import types as s_types
# from edb.schema import utils as s_utils

from edb.pgsql import ast as pgast
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

def analyze_explain_output(
    dbv: Any, # dbview.DatabaseConnectionView,
    compiled: Any, #dbview.CompiledQuery,
    data: bytes,
) -> bytes:
    debug.header('Explain')

    unit = compiled.query_unit_group[0]
    ql: qlast.Base
    ir: irast.Statement
    pg: pgast.Base
    ql, ir, pg = unit.query_asts
    assert len(data) == 1 and len(data[0]) == 1
    # print('DATA', data)
    plan = json.loads(data[0][0])

    print(dbv)
    ql.dump()
    ir.dump()
    debug.dump(plan)

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
