
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT r1.A as BOB, r2.B as JOE
FROM R r1, R r2
WHERE r1.B > r2.A
