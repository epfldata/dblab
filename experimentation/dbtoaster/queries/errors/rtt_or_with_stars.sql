-- there is a bug in calc_of_condition
-- it can be realated to "X"

CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT 
  t1.C AS C, t1.D AS D, 
  t2.C AS C, t2.D AS D
FROM 
  S AS S, 
  T t1, 
  T t2
WHERE 
  t1.D = t2.C 
    OR 
  2 = (S.B*t1.D)
;
