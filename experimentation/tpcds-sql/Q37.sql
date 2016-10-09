
SELECT  i_item_id
       ,i_item_desc
       ,i_current_price
 FROM item 
 INNER JOIN inventory ON inv_item_sk = i_item_sk
 INNER JOIN date_dim ON d_date_sk=inv_date_sk
 INNER JOIN catalog_sales ON cs_item_sk = i_item_sk
 WHERE i_current_price BETWEEN 36 AND 36 + 30
 AND d_date BETWEEN DATE '1998-04-06' AND DATE '1998-04-06' + 60
 AND i_manufact_id IN (746,802,854,823)
 AND inv_quantity_on_hand BETWEEN 100 AND 500
 GROUP BY i_item_id,i_item_desc,i_current_price
 ORDER BY i_item_id
 LIMIT 100;