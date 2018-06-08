CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT A / 2.0 FROM R WHERE NOT EXISTS (SELECT 2);
