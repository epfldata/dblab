CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT r1.A FROM R r1, R r2 WHERE NOT (R1.A=R2.A);
SELECT r1.A FROM R r1, R r2 WHERE R1.A != R2.A;
