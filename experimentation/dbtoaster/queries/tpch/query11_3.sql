INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT p.nationkey, p.partkey, SUM(p.value) AS QUERY11
FROM
  (
    SELECT s.nationkey, ps.partkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey
    GROUP BY ps.partkey, s.nationkey
  ) p,
  (
    SELECT s.nationkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey
    GROUP BY s.nationkey
  ) n
WHERE p.nationkey = n.nationkey
  AND (SELECT sum(ps.supplycost * ps.availqty)
       FROM  partsupp ps, supplier s
       WHERE ps.suppkey = s.suppkey AND
             s.nationkey = p.nationkey AND
             ps.partkey = p.partkey             
       ) > 0.001 * n.value
GROUP BY p.nationkey, p.partkey;

