-- Unsupported features for this query
--   ORDER BY (ignored)
--   HAVING (rewritten as a nested query)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT p.partkey, SUM(p.value) AS QUERY11
FROM
  (
    SELECT ps.partkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  partsupp ps, supplier s, nation n
    WHERE ps.suppkey = s.suppkey
      AND s.nationkey = n.nationkey
      AND n.name = 'GERMANY'
    GROUP BY ps.partkey
  ) p
WHERE p.value > (
    SELECT sum(ps.supplycost * ps.availqty) * 0.001
    FROM  partsupp ps, supplier s, nation n
    WHERE ps.suppkey = s.suppkey 
      AND s.nationkey = n.nationkey
      AND n.name = 'GERMANY'
  )
GROUP BY p.partkey;
