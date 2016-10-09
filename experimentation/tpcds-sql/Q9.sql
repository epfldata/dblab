
SELECT CASE WHEN (SELECT COUNT(*) 
                  FROM store_sales 
                  WHERE ss_quantity BETWEEN 1 AND 20) > 25437
            THEN (SELECT AVG(ss_ext_discount_amt) 
                  FROM store_sales 
                  WHERE ss_quantity BETWEEN 1 AND 20) 
            ELSE (SELECT AVG(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 1 AND 20) END,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 21 AND 40) > 22746
            THEN (SELECT AVG(ss_ext_discount_amt)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 21 AND 40) 
            ELSE (SELECT AVG(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 21 AND 40) END,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 41 AND 60) > 9387
            THEN (SELECT AVG(ss_ext_discount_amt)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 41 AND 60)
            ELSE (SELECT AVG(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 41 AND 60) END,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 61 AND 80) > 10098
            THEN (SELECT AVG(ss_ext_discount_amt)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 61 AND 80)
            ELSE (SELECT AVG(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 61 AND 80) END,
       CASE WHEN (SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 81 AND 100) > 18213
            THEN (SELECT AVG(ss_ext_discount_amt)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 81 AND 100)
            ELSE (SELECT AVG(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 81 AND 100) END
FROM reason
WHERE r_reason_sk = 1
;


