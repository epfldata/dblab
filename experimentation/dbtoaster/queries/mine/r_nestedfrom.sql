CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT p.A, sum(p.value) AS Q FROM (
   SELECT R.A, sum(R.B) AS value FROM R GROUP BY R.A
) p 
WHERE p.value > 5
GROUP BY p.A;
