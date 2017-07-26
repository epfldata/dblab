i = 0
for f in ~/ocaml/cornell_db_maybms/dbtoaster/compiler/alpha5/test/queries/simple/*.sql
 do
 echo $f
 ~/ocaml/cornell_db_maybms/dbtoaster/compiler/alpha5/bin/dbtoaster $f -l CALC > ~/out$i.calc
 i=$((i + 1)) 
 done
