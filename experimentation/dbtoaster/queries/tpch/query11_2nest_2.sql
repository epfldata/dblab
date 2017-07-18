INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT p.nationkey, p.partkey, SUM(p.value) AS QUERY11
FROM
  (
    SELECT s.nationkey, ps.partkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey 
      AND (s.suppkey IN (SELECT ps2.suppkey FROM partsupp ps2 WHERE ps2.partkey = 2))
    GROUP BY ps.partkey, s.nationkey
  ) p
WHERE p.value > (
    SELECT sum(ps.supplycost * ps.availqty) * 0.001
    FROM  partsupp ps, (
      SELECT s.suppkey, s.nationkey, SUM(1) 
      FROM supplier s2
      WHERE s2.nationkey = 3
    ) s
    WHERE ps.suppkey = s.suppkey AND s.nationkey = p.nationkey
  )
GROUP BY p.nationkey, p.partkey;
