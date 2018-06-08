CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;
  
CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED csv;

SELECT sum(A*C) AS agg1, sum(A+C) AS agg2 FROM R,S WHERE R.B=S.B;
