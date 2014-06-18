import pandas as pd
import helper as h

class Column:
  """
  Representation of a column and the profiling information
  """

  def _qexec(self, fld, group=None, order=None):
    c = self._con.cursor()

    if group != None:
      group = " group by %s" % group
    else:
      group = ""

    if order != None:
      order = " order by %s" % order
    else:
      order = ""

    query = "select %s from `%s`.`%s` %s %s" % (fld, self._table.db(), self._table.name(), group, order)
    c.execute(query);
    return c.fetchall()

  def __init__(self, name, type_name, con, table):
    self._name = name
    self._type_name = type_name
    self._con = con
    self._table = table

  def __repr__(self):
    return self._name

  def name(self):
    return self._name

  def _repr_html_(self):
    pass

  @classmethod
  def build(cls, data, con, table):
    return Column(data[0], data[1], con, table)

  def __eq__(self, other):
    return self._name == other._name and self._type_name == other._type_name

  @h.memoize
  def min(self):
    """
    :returns the minimum of the column
    """
    res = self._qexec("min(%s)" %(self._name))
    if len(res) > 0:
      self._min = res[0][0]
    return self._min

  @h.memoize
  def max(self):
    """
    :returns the maximum of the column
    """
    res = self._qexec("max(%s)" %(self._name))
    if len(res) > 0:
      self._max = res[0][0]
    return self._max

  @h.memoize
  def dcount(self):
    res = self._qexec("count(distinct %s)" % self._name)
    if len(res) > 0:
      self._dcount = res[0][0]
    return self._dcount

  @h.memoize
  def distribution(self, limit=1024):
    """
    Build the distribution of distinct values
    """
    res = self._qexec("%s, count(*) as __cnt" % self.name() ,group="%s" % self.name(), order="__cnt DESC LIMIT %d" %(limit))
    dist = []
    cnt = self._table.size()
    for i,r in enumerate(res):
      dist.append(list(r)+[i, r[1]/float(cnt)])

    self._distribution = pd.DataFrame(dist, columns=["value", "cnt", "r", "fraction"])
    self._distribution.index = self._distribution.r

    return self._distribution

  @h.memoize
  def most_frequent(self):
    res = self._qexec("%s, count(*) as __cnt" % self.name() ,group="%s" % self.name(), order="cnt DESC LIMIT 1")
    self._most_frequent = res[0][0]
    return self._most_frequent

  @h.memoize
  def least_frequent(self):
    res = self._qexec("%s, count(*) as cnt" % self.name() ,group="%s" % self.name(), order="cnt ASC LIMIT 1")
    self._least_frequent = res[0][0]
    return self._least_frequent


  def _repr_html_(self):
    funs = [self.min, self.max, self.dcount, self.most_frequent, self.least_frequent]
    return h.render_table(["Name", "Value"], [[x.__name__, x()] for x in funs])



class Table:
  """
  Generic Table Object

  This class provides simple access to the columns of the table. Most of the methods that perform actual data access
  are cached to avoid costly lookups.


  """

  def __init__(self, name, con, db="default"):
    self._db = db
    self._name = name
    self._connection = con

  def name(self):
    """
    :return: name of the table
    """
    return self._name;

  def db(self):
    """
    :return: name of the database used
    """
    return self._db

  def column(self, col):
    """
    Given either a column index or name return the column structure
    :param col: either index or name
    :return: column data structure
    """
    if type(col) is str:
      for c in self._cols:
        if c.name == col:
          return c
    else:
      return self._cols[col]

  @h.memoize
  def __len__(self):
    """
    :return: number of rows in the table
    """
    c = self._connection.cursor()
    c.execute("select count(*) from `%s`.`%s`" %(self._db, self._name))
    self._count = c.fetchall()[0][0]
    return self._count

  def size(self):
    """
    alias to __len__()
    :return:
    """
    return len(self)

  @h.memoize
  def columns(self):
    """
    :return: the list of column in this table
    """
    c = self._connection.cursor()
    c.execute("describe `%s`.`%s`" %(self._db, self._name))
    self._cols = []
    for col in c.fetchall():
      self._cols.append(Column.build(col, table=self, con=self._connection))
    return self._cols;

  def __getitem__(self, item):
    """
    Subscript access to the tables by name
    :param item:
    :return:
    """
    for x in self.columns():
      if x.name() == item:
        return x
    raise KeyError(item)

  def __dir__(self):
    return [x.name() for x in self.columns()]


  def __repr__(self):
    return "<Table: \"%s.%s\">" %(self._db, self._name)


  def __getattr__(self, item):
    for x in self.columns():
      if x.name() == item:
        return x
    raise AttributeError("'%s' object has no attribute '%s'" % (type(self).__name__, item))
