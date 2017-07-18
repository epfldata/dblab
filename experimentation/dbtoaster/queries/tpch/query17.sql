INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT SUM(l.extendedprice) / 7.0 AS avg_yearly
FROM   lineitem l, part p
WHERE  p.partkey = l.partkey
  AND  p.brand = 'Brand#23'
  AND  p.container = 'MED BOX'
  AND  l.quantity < (
          SELECT 0.2 * AVG(l2.quantity)
          FROM lineitem l2
          WHERE l2.partkey = p.partkey
       )
