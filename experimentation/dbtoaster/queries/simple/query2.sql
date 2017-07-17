-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)
--   MIN      (replaced with equivalent query)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT s.acctbal, s.name, n.name, p.partkey, p.mfgr, s.address, s.phone, 
       s.comment
FROM part p, supplier s, partsupp ps, nation n, region r
WHERE p.partkey = ps.partkey
  AND s.suppkey = ps.suppkey
  AND p.size = 15
  AND (p.type LIKE '%BRASS')
  AND s.nationkey = n.nationkey 
  AND n.regionkey = r.regionkey 
  AND r.name = 'EUROPE'
  AND (NOT EXISTS (SELECT 1
                   FROM partsupp ps2, supplier s2, nation n2, region r2
                   WHERE p.partkey = ps2.partkey
                     AND s2.suppkey = ps2.suppkey
                     AND s2.nationkey = n2.nationkey
                     AND n2.regionkey = r2.regionkey
                     AND r2.name = 'EUROPE'
                     AND ps2.supplycost < ps.supplycost));

--  AND ps.supplycost = (SELECT MIN(ps2.supplycost)
--                       FROM partsupp ps2, supplier s2, nation n2, region r2
--                       WHERE p.partkey = ps2.partkey
--                         AND s2.suppkey = ps2.suppkey
--                         AND s2.nationkey = n2.nationkey
--                         AND n2.regionkey = r2.regionkey
--                         AND r2.name = 'EUROPE');
