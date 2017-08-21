-- it's not in the ocaml queries

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT r1.A AS A FROM R r1 WHERE EXISTS (SELECT R2.A AS A FROM R r2);
