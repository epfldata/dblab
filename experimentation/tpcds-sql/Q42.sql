
SELECT  dt.d_year
    ,item.i_category_id
    ,item.i_category
    ,SUM(ss_ext_sales_price) sum_ss_ext_sales_price
 FROM date_dim dt
 INNER JOIN store_sales ON d_date_sk = ss_sold_date_sk
 INNER JOIN item ON ss_item_sk = i_item_sk
 WHERE item.i_manager_id = 1    
    AND dt.d_moy=12
    AND dt.d_year=2000
 GROUP BY   dt.d_year
        ,item.i_category_id
        ,item.i_category
 ORDER BY sum_ss_ext_sales_price DESC,dt.d_year
        ,item.i_category_id
        ,item.i_category
LIMIT 100;
