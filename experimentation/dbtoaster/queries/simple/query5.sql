-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)
--   INTERVAL (inlined into constant)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT n.name, SUM(l.extendedprice * (1 - l.discount)) AS revenue 
FROM   customer c, orders o, lineitem l, supplier s, nation n, region r
WHERE  c.custkey = o.custkey
  AND  l.orderkey = o.orderkey 
  AND  l.suppkey = s.suppkey
  AND  c.nationkey = s.nationkey 
  AND  s.nationkey = n.nationkey 
  AND  n.regionkey = r.regionkey 
  AND  r.name = 'ASIA'
  AND  o.orderdate >= DATE('1994-01-01')
  AND  o.orderdate <  DATE('1994-01-01') + interval '1' year 
GROUP BY n.name
