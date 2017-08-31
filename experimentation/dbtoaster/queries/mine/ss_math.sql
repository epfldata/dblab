CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT 
  s2.B, 
  s2.C, 
  (s2.B+(s1.B*s1.C)) AS result_1, 
  (s2.C*s2.B*(1*s2.B)) AS result_2, 
  s1.C AS result_3 
FROM S s1, S s2 
WHERE (NOT 2 = s2.C) AND 0 = s2.B;
