CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT A FROM R r1 WHERE EXISTS (SELECT R2.A FROM R r2);
