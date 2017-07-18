-- Unsupported features for this query
--   ORDER BY (ignored)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT s.name, s.address 
FROM supplier s, nation n
WHERE s.suppkey IN 
        ( SELECT ps.suppkey
          FROM partsupp ps
          WHERE ps.partkey IN ( SELECT p.partkey
                                FROM part p
                                WHERE p.name like 'forest%' )
          AND ps.availqty > ( SELECT 0.5 * SUM(l.quantity)
                              FROM lineitem l
                              WHERE l.partkey = ps.partkey
                              AND l.suppkey = ps.suppkey
                              AND l.shipdate >= DATE('1994-01-01')
                              AND l.shipdate < DATE('1995-01-01') ) )
AND s.nationkey = n.nationkey
AND n.name = 'CANADA'; 
