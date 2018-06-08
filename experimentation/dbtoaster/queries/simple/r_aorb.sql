
CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT A + 1 FROM R WHERE A = 1 OR B = 3;
