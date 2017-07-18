CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT r1.A, SUM(1)
FROM R r1
WHERE EXISTS (SELECT r2.B FROM R r2 WHERE r1.A > r2.A)
GROUP BY r1.A
