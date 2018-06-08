CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/standard/r.dat' LINE DELIMITED csv;

SELECT foo 
FROM (SELECT R.A AS foo, COUNT(*) FROM R GROUP BY foo) q;

SELECT foo 
FROM (SELECT R.A AS foo, COUNT(*) FROM R GROUP BY R.A) q;