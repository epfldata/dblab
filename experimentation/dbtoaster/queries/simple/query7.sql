-- Unsupported features for this query
--   ORDER BY (ignored)

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT supp_nation, cust_nation, l_year, SUM(volume) as revenue
FROM (
  SELECT n1.name AS supp_nation,
         n2.name AS cust_nation,
         (DATE_PART('year', l.shipdate)) AS l_year,
         l.extendedprice * (1 - l.discount) AS volume
  FROM supplier s, lineitem l, orders o, customer c, nation n1, nation n2
  WHERE s.suppkey = l.suppkey
    AND o.orderkey = l.orderkey
    AND c.custkey = o.custkey
    AND s.nationkey = n1.nationkey 
    AND c.nationkey = n2.nationkey 
    AND (
      (n1.name = 'FRANCE' and n2.name = 'GERMANY') 
        OR
      (n1.name = 'GERMANY' and n2.name = 'FRANCE')
    )
    AND (l.shipdate BETWEEN DATE('1995-01-01') AND DATE('1996-12-31') )
  ) AS shipping
GROUP BY supp_nation, cust_nation, l_year;
