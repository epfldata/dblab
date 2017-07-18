INCLUDE 'test/queries/tpch/schemas.sql';

SELECT *
FROM (
SELECT c.custkey AS c_custkey, COUNT(*) AS c_count
FROM customer c
GROUP BY c.custkey
) s;
