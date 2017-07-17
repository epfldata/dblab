-- Unsupported features for this query
--   CREATE VIEW (replaced with nested query)
--   Predicate r1.total_revenue = MAX(r2.total_revenue) replaced by 
--      (NOT EXISTS (SELECT 1 ... WHERE r2.total_revenue > r1.total_revenue))
--   ORDER BY (ignored)

/* We change the query to be more "incrementality friendly". In order to avoid
   issues with floating points, we cast the keys to integers. */

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT s.suppkey, s.name, s.address, s.phone, r1.total_revenue as total_revenue
FROM supplier s, 
     (SELECT l.suppkey AS supplier_no, 
             SUM(l.extendedprice * (1 - l.discount)) AS total_revenue
      FROM lineitem l
      WHERE l.shipdate >= DATE('1996-01-01')
        AND l.shipdate <  DATE('1996-04-01')
      GROUP BY l.suppkey) AS r1
WHERE 
    s.suppkey = r1.supplier_no
    AND (NOT EXISTS (SELECT 1
                     FROM (SELECT l.suppkey, 
                                  SUM(l.extendedprice * (1 - l.discount)) 
                                        AS total_revenue
                           FROM lineitem l
                           WHERE l.shipdate >= DATE('1996-01-01')
                             AND l.shipdate <  DATE('1996-04-01')
                           GROUP BY l.suppkey) AS r2
                     WHERE r2.total_revenue > r1.total_revenue) );
