Legobase
======

An efficient query engine in Scala programming language

Installation
============

LegoBase interpreter requeires Pardis Library (https://github.com/amirsh/pardis) project `pardis-library`.

LegoBase compiler requires Pardis Compiler (https://github.com/amirsh/pardis) project `pardis-compiler`.

Clone this project and checkout to the desired branch. Then, after going to sbt console, 
run `project pardis-library` for going to Pardis Library project and run `project pardis-compiler` for 
going to Pardis Compiler project.
run `publish-local` on the projects you need.

Testing
=======
For testing the interpreter, go to `lego-core` project using `project lego-core`.
Then for running query X with scaling factor N you have to run the following command:
`run DATA_FOLDER N QX`

For testing the compiler (Scala generated code), first you have to generate the code. 
For that purpose you have to go to `legocompiler` project using `project legocompiler`.
Then for generating query X with scaling factor N you have to run the following command:
`generate-test DATA_FOLDER N QX`
Then for testing the correctness you have copy the generated file into `test` folder of `legocompiler` project.
Then you have to run the following command:
`test:run DATA_FOLDER N QX`

For testing all TPCH queries with Scala code generator, in `legocompiler` project, 
you should run `generate-test DATA_FOLDER N testsuite`.
Then you should publish `lego-core` project using `lego-core/publish-local`.
Afterwards, you have set your the environment `SCALA_PATH` to the folder which contains `scalac`.
Finally, you have to go to `generator-out` folder and run `./run_scala.sh DATA_FOLDER N`.
