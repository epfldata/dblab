-- subq -> make_domain

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT N.A AS A, N.B AS B FROM (SELECT R.A AS A, R.B AS B FROM R AS R ) AS n;
