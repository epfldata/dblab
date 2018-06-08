-- Expected result: 
-- 1 -> 3
-- 2 -> 42
-- 3 -> 12
-- 4 -> 108
-- 5 -> 80

CREATE STREAM R(A float, B float) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT A, SUM(B * (SELECT SUM(r2.A) FROM R r2 WHERE r1.A = r2.A)) 
FROM R r1 GROUP BY A
