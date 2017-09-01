CREATE STREAM r(a INT);

SELECT r1.a, (SELECT count(*) FROM r r2) * sum(r1.a)
FROM r r1
GROUP BY r1.a

