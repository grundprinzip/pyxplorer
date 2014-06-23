import functools
from StringIO import StringIO


def car(data):
  return [x[0] for x in data]

def render_table(head, rows, limit=10):
  buf = StringIO()
  buf.write("<table><tr>")
  for h in head:
    buf.write("<th>{0}</th>".format(h))
  buf.write("</tr>")

  # Build the slices we need
  if limit == None or len(rows) <= limit:
    data = rows
    footer = None
  else:
    data = rows[:9]
    footer = rows[-1:]

  for r in data:
    buf.write("<tr>")
    for c in r:
      buf.write("<td>{0}</td>".format(c))
    buf.write("</tr>")


  if footer:
    for r in footer:
      buf.write("<tr>")
      for c in r:
        buf.write("<td>{0}</td>".format(c))
      buf.write("</tr>")
  buf.write("</table>")
  buf.write("<p>Rows: %d / Columns: %d</p>" % (len(rows), len(head)))
  return buf.getvalue()



def memoize(obj):
    cache = obj.cache = {}
    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]


    return memoizer
