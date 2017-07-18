-- Unsupported features for this query
--   ORDER BY (ignored)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT nation, o_year, SUM(amount) AS sum_profit 
FROM (
  SELECT n.name AS nation, 
         EXTRACT(year from o.orderdate) AS o_year,
         ((l.extendedprice * (1 - l.discount)) - (ps.supplycost * l.quantity))
            AS amount
  FROM   part p, supplier s, lineitem l, partsupp ps, orders o, nation n
  WHERE  s.suppkey = l.suppkey
    AND  ps.suppkey = l.suppkey 
    AND  ps.partkey = l.partkey
    AND  p.partkey = l.partkey
    AND  o.orderkey = l.orderkey 
    AND  s.nationkey = n.nationkey 
    AND  (p.name LIKE '%green%')
  ) AS profit 
GROUP BY nation, o_year;
