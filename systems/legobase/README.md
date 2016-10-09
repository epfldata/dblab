LegoBase
========

LegoBase is an efficient query engine in Scala programming language. This project
hosts the compilation pipeline of LegoBase.

Installation
============

By default, the latest binary version of SC is published and the installation is handled automatically by sbt.

However, if you have access to the [source of SC](https://github.com/epfldata/sc) you can manually install 
by following these steps:

1. Clone this project and checkout to the desired branch.

2. Run `bin/install_all.sh`.

sbt takes care of fetching the necessary dependencies. 


Testing
=======
In order to perform query compilation, first you have to go the Legobase compiler
project using `project legobase`. 
For generating the TPCH query X with scaling factor N, you have to run the following command:
`run DATA_FOLDER N QX`

This command requires optimization flags. The best combination of the flags can be specified 
by `-optimal` (the best combination of optimizations is specified in config/optimal.properties).
This means that for generating the C code for the query X with scaling factor N, you have to
run the following command:
`run DATA_FOLDER N QX -optimal`

For experimenting with other optimization flags, instead of generating the code by using the `-optimal`
you have to use the appropriate optimization flags. As an example, to apply the `HashMapGrouping`  and
`ColumnStore` optimizations, you have to run the following command:
`run DATA_FOLDER N QX +hm-part +cstore`

The detailed list of available optimization flags can be found by running `run`.

For generating Scala code you have to add the `-scala` flag. For example, in order to
generate the best Scala program, you have to run the following command:
`run DATA_FOLDER N QX -optimal -scala`

For some queries, a functional implementation is available. For generating those queries
you have to append `_functional` postfix to the name of queries. Also, you have to use
the `+monad-lowering` flag:
`run DATA_FOLDER N QX_functional +monad-lowering`

The generated C and Scala codes will be in the folder `generator-out`.
