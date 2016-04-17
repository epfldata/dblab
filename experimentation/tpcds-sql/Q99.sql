
SELECT  
   SUBSTRING(w_warehouse_name,1,20) as substr_w_warehouse_name
  ,sm_type
  ,cc_name
  ,SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30 ) THEN 1 ELSE 0 END) AS "30 days" 
  ,SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) AND 
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1 ELSE 0 END ) AS "31-60 days" 
  ,SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) AND 
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1 ELSE 0 END)  AS "61-90 days" 
  ,SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) AND
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days" 
  ,SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk  > 120) THEN 1 ELSE 0 END) AS ">120 days" 
FROM
   catalog_sales
  ,warehouse
  ,ship_mode
  ,call_center
  ,date_dim
WHERE
    d_month_seq BETWEEN 1212 AND 1212 + 11
AND cs_ship_date_sk   = d_date_sk
AND cs_warehouse_sk   = w_warehouse_sk
AND cs_ship_mode_sk   = sm_ship_mode_sk
AND cs_call_center_sk = cc_call_center_sk
GROUP BY
  substr_w_warehouse_name
  ,sm_type
  ,cc_name
ORDER BY substr_w_warehouse_name
        ,sm_type
        ,cc_name
LIMIT 100;


