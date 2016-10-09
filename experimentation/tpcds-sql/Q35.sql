
SELECT   
  ca_state,
  cd_gender,
  cd_marital_status,
  c_dep_count,
  COUNT(*) cnt1,
  AVG(cd_dep_count) avg_cd_dep_count,
  MAX(cd_dep_count) max_cd_dep_count,
  SUM(cd_dep_count) sum_cd_dep_count,
  cd_dep_employed_count,
  COUNT(*) cnt2,
  AVG(cd_dep_employed_count) avg_cd_dep_employed_count,
  MAX(cd_dep_employed_count) max_cd_dep_employed_count,
  SUM(cd_dep_employed_count) sum_cd_dep_employed_count,
  cd_dep_college_count,
  COUNT(*) cnt3,
  AVG(cd_dep_college_count) avg_cd_dep_college_count,
  MAX(cd_dep_college_count) max_cd_dep_college_count,
  SUM(cd_dep_college_count) sum_cd_dep_college_count
 FROM
  customer c,customer_address ca,customer_demographics
 WHERE
  c.c_current_addr_sk = ca.ca_address_sk AND
  cd_demo_sk = c.c_current_cdemo_sk AND 
  EXISTS (SELECT *
          FROM store_sales,date_dim
          WHERE c.c_customer_sk = ss_customer_sk AND
                ss_sold_date_sk = d_date_sk AND
                d_year = 1999 AND
                d_qoy < 4) AND
   (EXISTS (SELECT *
            FROM web_sales,date_dim
            WHERE c.c_customer_sk = ws_bill_customer_sk AND
                  ws_sold_date_sk = d_date_sk AND
                  d_year = 1999 AND
                  d_qoy < 4) OR 
    EXISTS (SELECT * 
            FROM catalog_sales,date_dim
            WHERE c.c_customer_sk = cs_ship_customer_sk AND
                  cs_sold_date_sk = d_date_sk AND
                  d_year = 1999 AND
                  d_qoy < 4))
 GROUP BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 ORDER BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 LIMIT 100;


