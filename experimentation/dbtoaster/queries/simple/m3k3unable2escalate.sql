CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();
  
SELECT 1 WHERE 5.0 IN (SELECT r3.A FROM R r3);
