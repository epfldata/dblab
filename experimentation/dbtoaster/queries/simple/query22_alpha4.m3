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
  FROM FILE '../../experiments/data/tpch/big/orders.csv'
  LINE DELIMITED CSV (delimiter := '|');

CREATE STREAM CUSTOMER (
        custkey      INT,
        name         VARCHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        mktsegment   CHAR(10),
        comment      VARCHAR(117)
    )
  FROM FILE '../../experiments/data/tpch/big/customer.csv'
  LINE DELIMITED CSV (delimiter := '|');

DECLARE MAP QUERY_1_1(float)[][ C1_NATIONKEY:int ] := AggSum([C1_NATIONKEY:int], 
  (CUSTOMER(C1_CUSTKEY:int, C1_NAME, C1_ADDRESS, C1_NATIONKEY:int, C1_PHONE,
              C1_ACCTBAL, C1_MKTSEGMENT, C1_COMMENT) *
    AggSum([], 
      ((__sql_inline_agg_2 ^=
         AggSum([], 
           (CUSTOMER(C2_CUSTKEY:int, C2_NAME, C2_ADDRESS, C2_NATIONKEY:int, C2_PHONE,
                       C2_ACCTBAL, C2_MKTSEGMENT, C2_COMMENT) *
             {C2_ACCTBAL > 0} * C2_ACCTBAL))) *
        {C1_ACCTBAL < __sql_inline_agg_2})) *
    AggSum([C1_CUSTKEY:int], 
      ((__sql_inline_agg_1 ^=
         AggSum([C1_CUSTKEY:int], 
           ORDERS(O_ORDERKEY:int, C1_CUSTKEY:int, O_ORDERSTATUS, O_TOTALPRICE,
                    O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY,
                    O_COMMENT))) *
        (__sql_inline_agg_1 ^= 0))) *
    C1_ACCTBAL));


DECLARE MAP CUSTOMER(float) [ ][ NATION:int, CKEY:int, ACCTBAL ] := 
  AggSum([NATION, CKEY, ACCTBAL], 
    CUSTOMER(CKEY:int, NAME, ADDRESS, NATION:int, PHONE, ACCTBAL, MKTSEG, COMMENT) * ACCTBAL
  );

DECLARE MAP BALANCE(float) [ ][ ] :=
  AggSum([], 
    CUSTOMER(CKEY:int, NAME, ADDRESS, NATION:int, PHONE, ACCTBAL, MKTSEG, COMMENT) * ACCTBAL
  );

DECLARE MAP ORDERS_BY_CUSTOMER(int) [  ][ CKEY:int ] := 
  AggSum([CKEY],
    ORDERS(OKEY:int, CKEY:int, STATUS, TOTAL, ODATE, OPTY, CLERK, SPTY, COMMENT)
  );

DECLARE QUERY QUERY22 := QUERY_1_1(float) [ ][ C1_NATIONKEY:int ];

-------------------------------------

ON + CUSTOMER ( CKEY:int, NAME, ADDRESS, NATION:int, PHONE, ACCTBAL, MKTSEG, COMMENT ) {

  CUSTOMER[][ NATION:int, CKEY:int, ACCTBAL ] += ACCTBAL;

  BALANCE[ ][ ] += ACCTBAL;
  
  QUERY_1_1[ ][ QNATION:int ]  := 
    AggSum([QNATION:int],
      CUSTOMER(float)[][ QNATION:int, QCKEY:int, QACCTBAL ] * 
        (tmp1 ^= BALANCE(float)[][]) *
        {QACCTBAL < tmp1} *
        (tmp2:int ^= ORDERS_BY_CUSTOMER(int)[][QCKEY:int]) *
        {tmp2:int = 0}
    );
}

ON - CUSTOMER ( CKEY:int, NAME, ADDRESS, NATION:int, PHONE, ACCTBAL, MKTSEG, COMMENT ) {

  CUSTOMER[][ NATION:int, CKEY:int, ACCTBAL ] += (-1.) * ACCTBAL;

  BALANCE[ ][ ] += (-1.) * ACCTBAL;
  
  QUERY_1_1[ ][ QNATION:int ]  := 
    AggSum([QNATION:int],
      CUSTOMER(float)[][ QNATION:int, QCKEY:int, QACCTBAL ] * 
        (tmp1 ^= BALANCE(float)[][]) *
        {QACCTBAL < tmp1} *
        (tmp2:int ^= ORDERS_BY_CUSTOMER(int)[][QCKEY:int]) *
        {tmp2:int = 0}
    );
}

ON + ORDERS ( OKEY:int, CKEY:int, STATUS, TOTAL, ODATE, OPTY, CLERK, SPTY, COMMENT ) {
  QUERY_1_1[ ][ QNATION:int ] +=
    AggSum([QNATION:int],
        CUSTOMER(float)[][ QNATION:int, CKEY:int, QACCTBAL ] * 
        (tmp1 ^= BALANCE(float)[][]) *
        {QACCTBAL < tmp1} *
        (tmp2:int ^= ORDERS_BY_CUSTOMER(int)[][CKEY:int] + 1) *
        {tmp2:int = 0}
    );
  QUERY_1_1[ ][ QNATION:int ] +=
    ( - AggSum([QNATION:int],
        CUSTOMER(float)[][ QNATION:int, CKEY:int, QACCTBAL ] * 
        (tmp1 ^= BALANCE(float)[][]) *
        {QACCTBAL < tmp1} *
        (tmp2:int ^= ORDERS_BY_CUSTOMER(int)[][CKEY:int]) *
        {tmp2:int = 0}
      )
    );
  
  ORDERS_BY_CUSTOMER[][ CKEY:int ] += 1;
}

ON - ORDERS ( OKEY:int, CKEY:int, STATUS, TOTAL, ODATE, OPTY, CLERK, SPTY, COMMENT ) {
  QUERY_1_1[ ][ QNATION:int ] +=
    ( AggSum([QNATION:int],
        CUSTOMER(float)[][ QNATION:int, CKEY:int, QACCTBAL ] * 
        (tmp1 ^= BALANCE(float)[][]) *
        {QACCTBAL < tmp1} *
        (tmp2:int ^= ORDERS_BY_CUSTOMER(int)[][CKEY:int] - 1) *
        {tmp2:int = 0}
      )
    );
  QUERY_1_1[ ][ QNATION:int ] +=
    ( - AggSum([QNATION:int],
        CUSTOMER(float)[][ QNATION:int, CKEY:int, QACCTBAL ] * 
        (tmp1 ^= BALANCE(float)[][]) *
        {QACCTBAL < tmp1} *
        (tmp2:int ^= ORDERS_BY_CUSTOMER(int)[][CKEY:int]) *
        {tmp2:int = 0}
      )
    );
  
  ORDERS_BY_CUSTOMER[][ CKEY:int ] += (-1);
}
