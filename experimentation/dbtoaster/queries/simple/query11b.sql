INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT SUM(ps.supplycost * ps.availqty) AS query11b
FROM  partsupp ps, supplier s
WHERE ps.suppkey = s.suppkey
