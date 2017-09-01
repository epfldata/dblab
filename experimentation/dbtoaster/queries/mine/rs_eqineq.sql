CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV ();

CREATE STREAM S(B int, C int)
FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
CSV ();

SELECT R.A AS A, R.B AS B, S.B AS B, S.C AS C FROM R AS R ,S AS S WHERE ((R.B) = (S.B)) AND ((R.A) < (S.C))
