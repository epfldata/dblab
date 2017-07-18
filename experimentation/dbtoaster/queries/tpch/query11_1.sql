INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT p.nationkey, p.partkey, SUM(p.value) AS QUERY11
FROM
  (
    SELECT s.nationkey, ps.partkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  (SELECT * FROM partsupp) ps, 
          (SELECT * FROM supplier) s
    WHERE ps.suppkey = s.suppkey
    GROUP BY ps.partkey, s.nationkey
  ) p
WHERE p.value > (
    SELECT sum(ps.supplycost * ps.availqty) * 0.001
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey AND s.nationkey = p.nationkey
  )
GROUP BY p.nationkey, p.partkey;
