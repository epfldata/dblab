CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV ();

SELECT * FROM R r2 WHERE r2.B >= SOME (SELECT r1.A FROM R r1);
