CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED csv;
  
CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED csv;

SELECT R.B, (X.S / X.C) AS avg_a FROM R, (SELECT Y.B, COUNT(*) AS C, SUM(Y.A) AS S FROM R AS Y GROUP BY Y.B) AS X WHERE R.B = X.B;

-- With tupling the raw calc (-d PRINT-RAW-CALC) should be something like:
--
-- AggSum([R_B, AVG_A], 
--  (R(R_A, R_B) *
--    (X ^= AggSum([X_B], (R(Y_A, Y_B) * <C: 1,  S: Y_A> * (X_B ^= Y_B)))) *
--    {R_B = X_B} * (AVG_A ^= (X.S * {[/:float](X.C)}))))
