-- Unsupported features for this query
--   ORDER BY (ignored)
--   LEFT OUTER JOIN (replaced with a natural join)
--   Multiple column renaming

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT c_count, COUNT(*) AS custdist
FROM (  
   SELECT c.custkey AS c_custkey, COUNT(o.orderkey) AS c_count
   FROM customer c, orders o
   WHERE c.custkey = o.custkey 
     AND (o.comment NOT LIKE '%special%requests%')
   GROUP BY c.custkey
) c_orders
GROUP BY c_count;
