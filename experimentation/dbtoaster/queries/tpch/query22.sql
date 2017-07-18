-- Unsupported features for this query
-- ORDER BY (ignored)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT  cntrycode,
        COUNT(*) AS numcust,
        SUM(custsale.acctbal) AS totalacctbal
FROM (
  SELECT SUBSTRING(c.phone, 0, 2) AS cntrycode,
         c.acctbal
  FROM   customer c
  WHERE  (SUBSTRING(c.phone, 0, 2) IN LIST 
              ('13', '31', '23', '29', '30', '18', '17'))
    AND  c.acctbal > (
            SELECT AVG(c2.acctbal)
            FROM   customer c2
            WHERE  c2.acctbal > 0.00
            AND    (SUBSTRING(c2.phone, 0, 2) IN LIST 
                        ('13', '31', '23', '29', '30', '18', '17')))
    AND  (NOT EXISTS (SELECT * FROM orders o WHERE o.custkey = c.custkey))
  ) as custsale
GROUP BY cntrycode
          
