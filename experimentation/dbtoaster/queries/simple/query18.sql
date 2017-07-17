-- Unsupported features for this query
--   ORDER BY (ignored)
--   HAVING (rewritten as a nested query)


INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, 
       sum(l.quantity) AS query18
FROM customer c, orders o, lineitem l
WHERE o.orderkey IN 
  ( SELECT l3.orderkey FROM (
      SELECT l2.orderkey, SUM(l2.quantity) AS QTY 
      FROM lineitem l2 GROUP BY l2.orderkey 
    ) l3
    WHERE QTY > 100
  )
 AND c.custkey = o.custkey
 AND o.orderkey = l.orderkey
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice;
