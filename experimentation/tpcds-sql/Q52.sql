
SELECT  dt.d_year
 	,item.i_brand_id
 	,item.i_brand
 	,SUM(ss_ext_sales_price) ext_price
 FROM date_dim dt
     ,store_sales
     ,item
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    AND store_sales.ss_item_sk = item.i_item_sk
    AND item.i_manager_id = 1
    AND dt.d_moy=12
    AND dt.d_year=1998
 GROUP BY dt.d_year
 	,item.i_brand
 	,item.i_brand_id
 ORDER BY dt.d_year
 	,ext_price DESC
 	,item.i_brand_id
LIMIT 100 ;


