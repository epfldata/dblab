-- Expected result: 
-- 1 -> 4
-- 2 -> 64
-- 3 -> 100
-- 4 -> 9
-- 5 -> 81

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT r1.B, SUM(r1.A * r2.A)
FROM R r1, R r2 
WHERE r1.B = r2.B
GROUP BY r1.B
