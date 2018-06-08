CREATE STREAM test (
  I int,
  S1 string,
  S2 string,
  D date,
  F float
);

SELECT cast_int(23.5), cast_date('1998-12-19'), cast_float('19.2');

SELECT cast_int(F), cast_date(S1), cast_float(S2), cast_string(D), cast_int(I)
FROM test