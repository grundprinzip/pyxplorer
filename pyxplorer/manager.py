import pandas as pd
from impala.dbapi import connect
import types as t
import helper as h

class Database:

  def __init__(self, db, host, port, username=None, password=None):
    self.db = db
    self.connection = connect(host=host, port=port)

  def __getitem__(self, item):
    for x in self.tables():
      if x.name() == item:
        return x
      raise KeyError(item)

  @h.memoize
  def tables(self):
    cursor = self.connection.cursor()
    cursor.execute("show tables in %s" % self.db)
    self._tables = [t.Table(r[0], con=self.connection, db=self.db) for r in cursor.fetchall()]
    return self._tables

  @h.memoize
  def tcounts(self):
    df = pd.DataFrame([[t.name(), t.size()] for t in self.tables()], columns=["name", "size"])
    df.index = df.name
    return  df
