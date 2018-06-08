-- tgt_expr <- expr_type 

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT (R.A + R.B) AS SUM1 FROM R AS R ;
