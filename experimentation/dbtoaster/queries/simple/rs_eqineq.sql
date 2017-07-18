CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV ();

CREATE STREAM S(B int, C int)
FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
CSV ();

SELECT * FROM R,S WHERE R.B = S.B AND R.A < S.C
