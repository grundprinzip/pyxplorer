import pandas as pd
from impala.dbapi import connect
import types as t
import helper as h



class Database:

  def __init__(self, db, conn):
    self.db = db
    self.connection = conn

  def __getitem__(self, item):
    for x in self.tables():
      if x.name() == item:
        return x
    raise KeyError(item)

  @h.memoize
  def tables(self):
    """
    :return: all tables stored in this database
    """
    cursor = self.connection.cursor()
    cursor.execute("show tables in %s" % self.db)
    self._tables = [t.Table(r[0], con=self.connection, db=self.db) for r in cursor.fetchall()]
    return self._tables

  @h.memoize
  def tcounts(self):
    """
    :return: a data frame containing the names and sizes for all tables
    """
    df = pd.DataFrame([[t.name(), t.size()] for t in self.tables()], columns=["name", "size"])
    df.index = df.name
    return  df

  @h.memoize
  def dcounts(self):
    """
    :return: a data frame with names and distinct counts and fractions for all columns in the database
    """
    data = []
    for t in self.tables():
      for c in t.columns():
        data.append([t.name(), c.name(), c.dcount(), t.size(), c.dcount() / float(t.size())])
    df = pd.DataFrame(data, columns=["table", "column", "distinct", "size", "fraction"])
    return df


  def _repr_html_(self):
    return h.render_table(["Name", "Size"], [[x.name(), x.size()] for x in self.tables()])
