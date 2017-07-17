-- Unsupported features for this query
--   INTERVAL (inlined into constant)

/* We insert a LISTMAX to support incremental computation.  For this particular 
   query, this is safe, because if the denominator equals 0, then the numerator 
   of the division is also guaranteed to be 0. */

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT (100.00 * SUM(CASE WHEN (p.type LIKE 'PROMO%') 
                     THEN l.extendedprice * (1 - l.discount) ELSE 0 END) / 
                 LISTMAX(1, SUM(l.extendedprice * (1 - l.discount)))) AS
                 promo_revenue
FROM lineitem l, part p
WHERE l.partkey = p.partkey
  AND l.shipdate >= DATE('1995-09-01') 
  AND l.shipdate <  DATE('1995-10-01')
