-- Unsupported features for this query
--   INTERVAL (inlined into constant)
--   ORDER BY (ignored)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT  p.brand,
        p.type,
        p.size,
        COUNT(DISTINCT ps.suppkey) AS supplier_cnt
FROM    partsupp ps, part p
WHERE   p.partkey = ps.partkey
  AND   p.brand <> 'Brand#45'
  AND   (p.type NOT LIKE 'MEDIUM POLISHED%')
  AND   (p.size IN LIST (49, 14, 23, 45, 19, 3, 36, 9))
  AND   (ps.suppkey NOT IN (
          SELECT s.suppkey
          FROM   supplier s
          WHERE  s.comment LIKE '%Customer%Complaints%'
        ))
GROUP BY p.brand, p.type, p.size;
