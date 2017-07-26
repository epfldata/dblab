CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT R1.A / 2.0 FROM R AS R1 WHERE R1.A < (SELECT 3);

