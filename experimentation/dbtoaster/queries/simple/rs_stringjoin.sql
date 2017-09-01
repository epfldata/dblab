-- Expected result: 
-- 1,1 -> 1
-- 1,3 -> 3
-- 2,1 -> 2
-- 2,3 -> 6
-- 3,1 -> 1
-- 3,3 -> 3
-- 3,4 -> 1
-- 4,1 -> 1
-- 4,2 -> 2
-- 4,3 -> 5
-- 4,4 -> 1
-- 4,5 -> 1
-- 5,1 -> 2
-- 5,2 -> 2
-- 5,3 -> 6
-- 5,4 -> 1
-- 5,5 -> 1


CREATE STREAM R(A int, B string) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

CREATE STREAM S(B string, C int) 
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
  CSV ();

SELECT r.A, SUM(s.C)
FROM R r, S s
WHERE r.B = S.B
GROUP BY r.A;
