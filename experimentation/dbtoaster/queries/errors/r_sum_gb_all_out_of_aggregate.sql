-- tgt_expr <- expr_type 
CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV ();

SELECT A+SUM(1) FROM R GROUP BY A,B
