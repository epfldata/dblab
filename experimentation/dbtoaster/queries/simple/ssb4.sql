INCLUDE '../alpha5/test/queries/tpch/schemas.sql';

 SELECT sn.regionkey, 
        cn.regionkey,
        PART.type,
        SUM(LINEITEM.quantity) AS ssb4
 FROM   CUSTOMER, ORDERS, LINEITEM, PART, SUPPLIER, NATION cn, NATION sn
 WHERE  CUSTOMER.custkey = ORDERS.custkey
   AND  ORDERS.orderkey = LINEITEM.orderkey
   AND  PART.partkey = LINEITEM.partkey
   AND  SUPPLIER.suppkey = LINEITEM.suppkey
   AND  ORDERS.orderdate >= DATE('1997-01-01')
   AND  ORDERS.orderdate <  DATE('1998-01-01')
   AND  cn.nationkey = CUSTOMER.nationkey
   AND  sn.nationkey = SUPPLIER.nationkey
 GROUP BY sn.regionkey, cn.regionkey, PART.type
