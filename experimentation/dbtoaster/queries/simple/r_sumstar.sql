-- Expected result: 
-- 1 -> 4
-- 2 -> 32
-- 3 -> 80
-- 4 -> 6
-- 5 -> 36

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT r1.B, SUM(r1.A + r2.A)
FROM R r1, R r2 
WHERE r1.B = r2.B
GROUP BY r1.B
