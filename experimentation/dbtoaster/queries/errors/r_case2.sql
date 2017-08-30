-- func and case in calc_of_sql_expr

CREATE STREAM R(A int, B int, C int) 
  FROM FILE './r.dat' LINE DELIMITED csv;

SELECT total.YER, SUM(CASE total.NAME WHEN 42 THEN total.VOLUME ELSE 0 END) / 
         LISTMAX(1, SUM(total.VOLUME)) AS mkt_share_1
FROM (
  SELECT R.A as YER, R.B as NAME, R.C AS VOLUME
  FROM R
) total
GROUP BY total.YER;

SELECT R.A, SUM(CASE R.B WHEN 42 THEN R.C ELSE 0 END) / 
         LISTMAX(1, SUM(R.C)) AS mkt_share_2
FROM R
GROUP BY R.A;
