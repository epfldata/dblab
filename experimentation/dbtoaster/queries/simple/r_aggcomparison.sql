
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT *
FROM R r1, (SELECT SUM(r3.B) AS C FROM R r3) S
WHERE r1.A > (SELECT SUM(C) FROM R r2);
