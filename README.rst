pyxplorer -- Easy Interactive Data Profiling for Big Data (and Small Data)
--------------------------------------------------------------------------

The goal of pyxplorer is to provide a simple tool that allows interactive
profiling of datasets that are accessible via a SQL like interface. The only
requirement to run data profiling is that you are able to provide a Python
DBAPI like interface to your data source and the data source is able to
understand simplistic SQL queries.

I built this piece of software while trying to get a better understanding of
data distribution in a massive several hundred million record large dataset.
Depending on the size of the dataset and the query engine the resposne time
can ranging from seconds (Impala) to minutes (Hive) or even hourse (MySQL)

The typical use case is to use ```pyxplorer``` interactively from an iPython
Notebook or iPython shell to incrementally extract information about your data.

Usage
------

Imagine that you are provided with access to a huge Hive/Impala database on
your very own Hadoop cluster and you're asked to profile the data to get a
better understanding for performing more specific data science later on.::

  import pyxplorer as pxp
  from impala.dbapi import connect
  conn = connect(host='impala_server', port=21050)

  db = pxp.Database("default", conn)
  db.tables()

This simple code gives you access to all the tables in this database. So let's
assume the result shows a ```sales_orders``` table, what can we do now?::

  orders = db["sales_orders"]
  orders.size() # 100M
  orders.columns() # [ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ...]

Ok, if we have so many columns, what can we find out about a single column?::

  orders.ol_d_id.min() # 1
  orders.ol_d_id.max() # 9999
  orders.ol_d_id.dcount() # 1000

And like this there are some more key-figures about the data like uniqueness,
constancy, most and least frequent values and distribution.

In some cases, where it makes sense, the output of a method call will not be a
simple array or list but directly a Pandas dataframe to facilitate plotting
and further analysis.

You will find an easier to digest tutorial here:

  * http://nbviewer.ipython.org/github/grundprinzip/pyxplorer/blob/master/pyxplorer_stuff.ipynb


Supported Features
-------------------

  * Column Count (Database / Table)
  * Table Count
  * Tuple Count (Database / Table)
  * Min / Max
  * Most Frequent / Least Frequent
  * Top-K Most Frequent / Top-K Least Frequent
  * Top-K Value Distribution (Database / Table )
  * Uniqueness
  * Constancy
  * Distinc Value Count


Supported Platforms
--------------------

The following platforms are typically tested while using ```pyxplorer```

 * Hive
 * Impala
 * MySQL


Dependencies
-------------

  * pandas
  * phys2 for Hive based loading of data sets
  * pympala for connecting to Impala
  * snakebite for loading data from HDFS to Hive
