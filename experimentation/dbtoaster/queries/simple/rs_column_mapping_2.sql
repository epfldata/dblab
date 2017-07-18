-- Check if mapping produced in Calculus.cmp_expr is correct. 

CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

CREATE STREAM S(C int, D int)
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED csv;

SELECT r1.A, r1.B FROM S s, R r1, R r2 WHERE s.C=r1.A AND s.D = r2.A;
