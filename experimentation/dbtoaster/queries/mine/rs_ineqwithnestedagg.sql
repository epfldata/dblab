-- Expected result: 

CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
  CSV ();

SELECT A FROM R r, (SELECT S.B, COUNT(*) FROM S GROUP BY S.B) s2 WHERE r.B < s2.B;
