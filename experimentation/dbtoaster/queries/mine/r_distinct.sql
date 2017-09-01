--namer should name the R.A to A not to Var1

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT A, B FROM R;

SELECT DISTINCT A , B FROM R;
