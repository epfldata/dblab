-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)
--   INTERVAL (inlined into constant)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT o.orderpriority, COUNT(*) AS order_count
FROM   orders o
WHERE  o.orderdate >= DATE('1993-07-01')
  AND  o.orderdate <  DATE('1993-10-01')
  AND (EXISTS (
    SELECT * FROM lineitem l
    WHERE l.orderkey = o.orderkey
      AND l.commitdate < l.receiptdate
  ))
GROUP BY o.orderpriority;
