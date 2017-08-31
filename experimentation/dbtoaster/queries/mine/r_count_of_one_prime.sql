CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;

SELECT *
FROM (SELECT R.A, COUNT(*) as C FROM R GROUP BY R.A) s
WHERE s.A = 3
