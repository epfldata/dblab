INCLUDE '../alpha5/test/queries/tpch/schemas.sql'; 

SELECT SUM(l.extendedprice * (1 - l.discount) ) AS revenue
FROM lineitem l, part p
WHERE
    (
        p.partkey = l.partkey
        AND p.brand = 'Brand#12'
        AND ( p.container IN LIST ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') )
        AND l.quantity >= 1 AND l.quantity <= 1 + 10 
        AND ( p.size BETWEEN 1 AND 5 )
        AND (l.shipmode IN LIST ('AIR', 'AIR REG') )
        AND l.shipinstruct = 'DELIVER IN PERSON' 
    )
    OR 
    (
        p.partkey = l.partkey
        AND p.brand = 'Brand#23'
        AND ( p.container IN LIST ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') )
        AND l.quantity >= 10 AND l.quantity <= 10 + 10
        AND ( p.size BETWEEN 1 AND 10 )
        AND ( l.shipmode IN LIST ('AIR', 'AIR REG') )
        AND l.shipinstruct = 'DELIVER IN PERSON'
    )
    OR 
    (
        p.partkey = l.partkey
        AND p.brand = 'Brand#34'
        AND ( p.container IN LIST ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') )
        AND l.quantity >= 20 AND l.quantity <= 20 + 10
        AND ( p.size BETWEEN 1 AND 15 )
        AND ( l.shipmode IN LIST ('AIR', 'AIR REG') )
        AND l.shipinstruct = 'DELIVER IN PERSON'
    );
