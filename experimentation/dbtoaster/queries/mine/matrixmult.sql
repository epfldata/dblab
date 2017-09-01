create stream A(i INT, j INT, aij INT)
  FROM FILE 'a.dat' LINE DELIMITED csv;

create stream B(j INT, k INT, bjk INT)
  FROM FILE 'b.dat' LINE DELIMITED csv;

select i, k, sum(aij * bjk)
from A, B
where A.j = B.j
group by i, k


