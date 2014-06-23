from __future__ import print_function

import pandas as pd
import helper as h
import sys


class Column:
    """
    Representation of a column and the profiling information
    """

    def _qexec(self, fld, group=None, order=None):
        c = self._con.cursor()
        if not group is None:
            group = " group by %s" % group
        else:
            group = ""

        if not order is None:
            order = " order by %s" % order
        else:
            order = ""

        query = "select %s from `%s`.`%s` %s %s" % (fld, self._table.db(), self._table.name(), group, order)
        c.execute(query)
        return c.fetchall()

    def __init__(self, name, type_name, con, table):
        self._name = name
        self._type_name = type_name
        self._con = con
        self._table = table
        self._distribution = None
        self._min = None
        self._max = None
        self._dcount = None
        self._most_frequent = None
        self._most_frequent_count = None
        self._least_frequent = None
        self._least_frequent_count = None

    def __repr__(self):
        return self.name()

    def __str__(self):
        buf = "%s\n" % self.name()
        funs = [self.min, self.max, self.dcount, self.most_frequent, self.least_frequent]
        for x in funs:
            buf += "%s:\t%s\n" % (x.__name__, x())
        return buf

    def name(self):
        return self._name

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
        res = self._qexec("min(%s)" % self._name)
        if len(res) > 0:
            self._min = res[0][0]
        return self._min

    @h.memoize
    def max(self):
        """
        :returns the maximum of the column
        """
        res = self._qexec("max(%s)" % self._name)
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
        res = self._qexec("%s, count(*) as __cnt" % self.name(), group="%s" % self.name(),
                          order="__cnt DESC LIMIT %d" % limit)
        dist = []
        cnt = self._table.size()
        for i, r in enumerate(res):
            dist.append(list(r) + [i, r[1] / float(cnt)])

        self._distribution = pd.DataFrame(dist, columns=["value", "cnt", "r", "fraction"])
        self._distribution.index = self._distribution.r

        return self._distribution

    def most_frequent(self):
        res = self.n_most_frequent(1)
        self._most_frequent = res[0][0]
        self._most_frequent_count = res[0][1]
        return self._most_frequent, self._most_frequent_count

    def least_frequent(self):
        res = self.n_least_frequent(1)
        self._least_frequent = res[0][0]
        self._least_frequent_count = res[0][1]
        return self._least_frequent, self._least_frequent_count

    @h.memoize
    def n_most_frequent(self, limit=10):
        res = self._qexec("%s, count(*) as __cnt" % self.name(), group="%s" % self.name(),
                          order="__cnt DESC LIMIT %d" % limit)
        return res

    def n_least_frequent(self, limit=10):
        res = self._qexec("%s, count(*) as cnt" % self.name(), group="%s" % self.name(),
                          order="cnt ASC LIMIT %d" % limit)
        return res

    def size(self):
        return self._table.size()

    def uniqueness(self):
        return self.dcount() / float(self.size())

    def constancy(self):
        tup = self.most_frequent()
        return tup[1] / float(self.size())

    def _repr_html_(self):

        funs = [("Min", self.min), ("Max", self.max), ("#Distinct Values", self.dcount),
        ("Most Frequent", lambda: "{0} ({1})".format(*self.most_frequent())),
        ("Least Frequent", lambda: "{0} ({1})".format(*self.least_frequent())),
        ("Top 10 MF", lambda: ",".join(map(str, h.car(self.n_most_frequent())))),
        ("Top 10 LF", lambda: ", ".join(map(str, h.car(self.n_least_frequent())))),
        ("Uniqueness", self.uniqueness),
        ("Constancy", self.constancy),
        ]
        return h.render_table(["Name", "Value"], [[x[0], x[1]()] for x in funs])


class Table:
    """
    Generic Table Object

    This class provides simple access to the columns of the table. Most of the methods that perform actual data access
    are cached to avoid costly lookups.


    """

    def __init__(self, name, con, db="default"):
        self._cols = []
        self._db = db
        self._name = name
        self._connection = con

    def name(self):
        """
        :return: name of the table
        """
        return self._name

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
        c.execute("select count(*) from `%s`.`%s`" % (self._db, self._name))
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
        c.execute("describe `%s`.`%s`" % (self._db, self._name))
        self._cols = []
        for col in c.fetchall():
            self._cols.append(Column.build(col, table=self, con=self._connection))
        return self._cols

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
        """
        :return: an array of custom attributes, for code-completion in ipython
        """
        return [x.name() for x in self.columns()]

    def __repr__(self):
        return "<Table: \"%s.%s\">" % (self._db, self._name)

    def __getattr__(self, item):
        """
        :param item: name of the column
        :return: column object for attribute-like access to the column
        """
        for x in self.columns():
            if x.name() == item:
                return x
        raise AttributeError("'%s' object has no attribute '%s'" % (type(self).__name__, item))

    def num_columns(self):
        """
        :return: number of columns of the table
        """
        return len(self.columns())

    def distinct_value_fractions(self):
        """
        :return: returns a data frame of name distinct value fractions
        """
        return pd.DataFrame([c.dcount() / float(self.size()) for c in self.columns()],
                            index=[c.name() for c in self.columns()], columns=["fraction"])
