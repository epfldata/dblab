-- Unsupported features for this query
--   INTERVAL (inlined into constant)

/* Note that this query will fail to produce the correct answer on the OCaml  
   interpreter due to a floating point error in OCaml itself.  Specifically, 
   in OCaml, 0.06+0.01 <> 0.07.  This can not be helped. */

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT SUM(l.extendedprice*l.discount) AS revenue
FROM   lineitem l
WHERE  l.shipdate >= DATE('1994-01-01')
  AND  l.shipdate < DATE('1995-01-01')
  AND  (l.discount BETWEEN (0.06 - 0.01) AND (0.06 + 0.01)) 
  AND  l.quantity < 24;
