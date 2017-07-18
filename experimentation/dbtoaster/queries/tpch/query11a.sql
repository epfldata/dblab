INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT ps.partkey, SUM(ps.supplycost * ps.availqty) AS query11a
FROM  partsupp ps, supplier s
WHERE ps.suppkey = s.suppkey
GROUP BY ps.partkey;
