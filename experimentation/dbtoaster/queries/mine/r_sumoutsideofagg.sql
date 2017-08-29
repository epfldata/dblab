-- tgt_expr <- expr_type

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  csv ();

SELECT A, A+SUM(B) FROM R GROUP BY A;
