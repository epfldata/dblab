CC=clang
OPTFLAG=-O3
LIBS=-I/usr/local/include/glib-2.0/ -I/usr/local/lib/glib-2.0/include -L/usr/local/lib -lglib-2.0
CFLAGS=-g

FILES=$(wildcard *.c)
#FILES=Q1.c

default: compile

compile:
	for file in $(FILES); 															   \
	do 																				   \
		output=`echo $$file | cut -d'.' -f1`".out"; 								   \
		start=`date +'%H 3600 * %M 60 * + %S + 1000 * %N 1000000 / + p' | dc`;		   \
		$(CC) $(OPTFLAG) $(CFLAGS) $(LIBS) $$file -o $$output 1> error_warn_log.txt 2> error_warn_log.txt;		   \
		end=`date +'%H 3600 * %M 60 * + %S + 1000 * %N 1000000 / + p' | dc`; 		   \
		echo "Compiling C code for query $$file using Clang took "`echo "$$end - $$start" | bc`" msec"; \
	done

clean:
	rm *.out *.output

# ONLY CLANG
compile-opt:
	echo test "Compiling with CLANG and Optimizations of Neumann!"						   \
	for file in $(FILES); 															   \
	do 																				   \
		output=`echo $$file | cut -d'.' -f1`".out"; 								   \
		start=`date +'%H 3600 * %M 60 * + %S + 1000 * %N 1000000 / + p' | dc`;		   \
		clang -g -emit-llvm -o - -O3 -I/usr/local/include/glib-2.0/ -I/usr/local/lib/glib-2.0/include -c $$file | opt -instcombine -reassociate -gvn -simplifycfg -adce -simplifycfg -o - - | clang -x ir - -c -o $$output 1> error_warn_log.txt 2> error_warn_log.txt;		   \
		end=`date +'%H 3600 * %M 60 * + %S + 1000 * %N 1000000 / + p' | dc`; 		   \
		echo "Compiling C code for query $$file using Clang took "`echo "$$end - $$start" | bc`" msec"; \
	done
