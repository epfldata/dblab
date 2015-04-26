Legobase
======

An efficient query engine in Scala programming language

Installation
============

LegoBase interpreter requeires SC Pardis Library (https://github.com/epfldata/sc) project `sc-pardis-library`.

LegoBase compiler requires SC Pardis Compiler (https://github.com/epfldata/sc), C.Scala Library and C.Scala Compiler; 
the projects `sc-pardis-compiler`, `sc-c-scala-lib`, and `sc-c-scala-deep`.

If you have access to the sc repository follow these steps:
1. Clone this project and checkout to the desired branch. Then, after going to sbt console, 
run `project sc-pardis-library` for going to Pardis Library project and run `project sc-pardis-compiler` for 
going to Pardis Compiler project.
2. Run `publish-local` on the `root` project of Pardis.
Then run `sc-c-scala-lib/publish-local` and `sc-c-scala-deep/publish-local` in the sbt console
of Pardis project.

Otherwise, sbt takes care of fetching the necessary dependencies. 


Testing
=======
For testing the interpreter, go to `lego-core` project using `project lego-core`.
Then for running query X with scaling factor N you have to run the following command:
`run DATA_FOLDER N QX`

For testing the compiler (Scala generated code), first you have to generate the code. 
For that purpose you have to go to `lego-compiler` project using `project lego-compiler`.
Then for generating query X with scaling factor N you have to run the following command:
`generate-test DATA_FOLDER N QX`
Then for testing the correctness you have copy the generated file into `test` folder of `lego-compiler` project.
Then you have to run the following command:
`test:run DATA_FOLDER N QX`

For testing all TPCH queries with Scala code generator, in `lego-compiler` project, 
you should run `generate-test DATA_FOLDER N testsuite`.
Then you should publish `lego-core` project using `lego-core/publish-local`.
Afterwards, you have set your the environment `SCALA_PATH` to the folder which contains `scalac`.
Finally, you have to go to `generator-out` folder and run `./run_scala.sh DATA_FOLDER N`.
