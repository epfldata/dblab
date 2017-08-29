CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV ();

SELECT r3.A
FROM (SELECT r1.A FROM R r1,
(SELECT r2.A FROM R r2) r2b WHERE r1.A=r2b.A) r3,
R r4
WHERE r3.A = r4.A
