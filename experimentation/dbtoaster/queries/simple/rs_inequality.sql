CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  csv ();

CREATE STREAM S(C int, D int) 
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
  csv ();

--SELECT sum(A*H) FROM R,S,T,U WHERE R.B<S.C AND R.B<T.E AND T.F<U.G AND R.B<U.H;
SELECT sum(A*D) FROM R,S WHERE R.B<S.C;
