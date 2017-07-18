-- With the datafiles in ../../experiments/data, this should produce
-- QUERY_1_1 = 18753367048934
-- or as the Ocaml outputter puts it:
-- QUERY_1_1: [[  ]->[[  ]->1.87533670489e+13;]<pat=>;]<pat=>

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/standard/r.dat' LINE DELIMITED csv;

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/simple/standard/s.dat' LINE DELIMITED csv;

CREATE STREAM T(C int, D int)
  FROM FILE '../../experiments/data/simple/standard/t.dat' LINE DELIMITED csv;

SELECT sum(A*D) AS AtimesD FROM R,S,T WHERE R.B=S.B AND S.C=T.C;
