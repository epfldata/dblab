/* Result:
    sum    
-----------
 898778.73
(1 row)
 */

INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

SELECT sum(l.extendedprice) AS query17
FROM   lineitem l, part p
WHERE  p.partkey = l.partkey
AND    l.quantity < 0.005 *
       (SELECT sum(l2.quantity)
        FROM lineitem l2 WHERE l2.partkey = p.partkey);
