CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV ();

SELECT * FROM R WHERE R.A = R.B AND R.A <= R.B AND R.A >= R.B;
