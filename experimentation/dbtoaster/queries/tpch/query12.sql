-- Unsupported features for this query
--   INTERVAL (inlined into constant)
--   ORDER BY (ignored)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT l.shipmode, 
       SUM(CASE WHEN o.orderpriority IN LIST ('1-URGENT', '2-HIGH')
                THEN 1 ELSE 0 END) AS high_line_count,
       SUM(CASE WHEN o.orderpriority NOT IN LIST ('1-URGENT', '2-HIGH')
                THEN 1 ELSE 0 END) AS low_line_count
FROM   orders o, lineitem l
WHERE  o.orderkey = l.orderkey
  AND  (l.shipmode IN LIST ('MAIL', 'SHIP'))
  AND  l.commitdate < l.receiptdate
  AND  l.shipdate < l.commitdate
  AND  l.receiptdate >= DATE('1994-01-01')
  AND  l.receiptdate < DATE('1995-01-01')
GROUP BY l.shipmode;
