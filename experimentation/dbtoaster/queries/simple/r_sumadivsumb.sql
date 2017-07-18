-- Expected result: 1.

CREATE STREAM R(A float, B float) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

/* We insert a LISTMAX to support incremental computation. */

SELECT SUM(A)/LISTMAX(1, 1+SUM(B)) FROM R

--SELECT SUM(A)/(1+SUM(B)) FROM R
