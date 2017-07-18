-- Unsupported features for this query
--   ORDER BY (ignored)

/* We insert a LISTMAX to support incremental computation.  For this particular 
   query, this is safe, because if total.volume == 0, then the numerator of the
   division is also guaranteed to be 0. */


INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT  total.o_year,
        (SUM(CASE total.name WHEN 'BRAZIL' THEN total.volume ELSE 0 END) / 
         LISTMAX(1, SUM(total.volume))) AS mkt_share
FROM
  (
    SELECT n2.name,
           DATE_PART('year', o.orderdate) AS o_year,
           l.extendedprice * (1-l.discount) AS volume
    FROM   part p, supplier s, lineitem l, orders o, customer c, nation n1,
           nation n2, region r
    WHERE  p.partkey = l.partkey
      AND  s.suppkey = l.suppkey
      AND  l.orderkey = o.orderkey
      AND  o.custkey = c.custkey
      AND  c.nationkey = n1.nationkey 
      AND  n1.regionkey = r.regionkey 
      AND  r.name = 'AMERICA'
      AND  s.nationkey = n2.nationkey
      AND  (o.orderdate BETWEEN DATE('1995-01-01') AND DATE('1996-12-31'))
      AND  p.type = 'ECONOMY ANODIZED STEEL'
  ) total
GROUP BY total.o_year;
