
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT SUM(1) FROM (SELECT SUM(1) FROM R) r;
