-- Unsupported features for this query
--   INTERVAL (inlined into constant)
--   ORDER BY (ignored)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT  c.custkey, c.name, 
        c.acctbal,
        n.name,
        c.address,
        c.phone,
        c.comment,
        SUM(l.extendedprice * (1 - l.discount)) AS revenue
FROM    customer c, orders o, lineitem l, nation n
WHERE   c.custkey = o.custkey
  AND   l.orderkey = o.orderkey
  AND   o.orderdate >= DATE('1993-10-01')
  AND   o.orderdate < DATE('1994-01-01')
  AND   l.returnflag = 'R'
  AND   c.nationkey = n.nationkey
GROUP BY c.custkey, c.name, c.acctbal, c.phone, n.name, c.address, c.comment
