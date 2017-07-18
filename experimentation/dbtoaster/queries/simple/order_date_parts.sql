CREATE STREAM ORDERS (
        orderkey       INT,
        custkey        INT,
        orderstatus    CHAR(1),
        totalprice     DECIMAL,
        orderdate      DATE,
        orderpriority  CHAR(15),
        clerk          CHAR(15),
        shippriority   INT,
        comment        VARCHAR(79)
    )
  FROM FILE '../../experiments/data/tpch/tiny/orders.csv'
  LINE DELIMITED CSV (delimiter := '|');

SELECT EXTRACT(year FROM orderdate) AS Y,
       EXTRACT(month FROM orderdate) AS M,
       EXTRACT(day FROM orderdate) AS D
FROM   ORDERS;
