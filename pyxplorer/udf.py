__author__ = 'grund'

import impala.udf as udf
from numba import jit

import re

from numba import types as ntypes
from numba.typing.templates import Registry

types = ["FLOAT", "INTEGER", "DATE", "TIME", "DATETIME", "STRING"]


registry = Registry()
registry.register_global(re, ntypes.Module(re))


@udf.udf(udf.IntVal(udf.FunctionContext, udf.StringVal))
def check_types(context, value):
  """
  The goal of this function is to check a data type for a given function and return
  matched type per row.

  We need simple matching routines that do not rely on external modules

  :param context:
  :param value:
  :return:
  """
  valid_float = True
  valid_int = True
  valid_string = True

  return 0




def ship_functions(con, hdfs_path, name_node, name_node_port=50070):

  c = con.cursor()
  udf.ship_udf(f, check_types, hdfs_path, name_node, overwrite=True)
