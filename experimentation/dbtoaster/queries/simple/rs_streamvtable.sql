CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV ();

CREATE TABLE S(B int, C int)
FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
CSV ();

SELECT * FROM R NATURAL JOIN S;
