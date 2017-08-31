CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT r1.A FROM R r1 WHERE r1.A =
(SELECT SUM(r2.B)*0.5 FROM R r2 WHERE r2.A = r1.B);
