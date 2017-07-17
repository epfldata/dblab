INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT c1.nationkey, sum(c1.acctbal) AS query22
FROM customer c1
WHERE c1.acctbal <
    (SELECT sum(c2.acctbal) FROM customer c2 WHERE c2.acctbal > 0)
AND 0 = (SELECT sum(1) FROM orders o WHERE o.custkey = c1.custkey)
GROUP BY c1.nationkey
