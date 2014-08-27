Legobase
======

An efficient query engine in Scala programming language

Installation
============

LegoBase interpreter requeires Pardis Library (https://github.com/amirsh/pardis) project `pardis-library`.

LegoBase compiler requires Pardis Compiler (https://github.com/amirsh/pardis) project `pardis-core`.

Clone this project and checkout to the desired branch. Then, after going to sbt console, 
run `project pardis-library` for going to Pardis Library project and run `project pardis-core` for 
going to Pardis Compiler project.
run `publish-local` on the projects you need.

Testing
=======
For testing shallow version, go to `lego-core` project using `project lego-core`.
Then for running query X with scaling factor N you have to run the following command:
`run DATA_FOLDER N QX`

For testing deep version, first you have to generate the code. 
For that purpose you have to go to `legocompiler` project using `project legocompiler`.
Then for generating query X with scaling factor N you have to run the following command:
`generate-test DATA_FOLDER N QX`
Then for testing the correctness you have copy the generated file into `test` folder of `legocompiler` project.
Then you have to run the following command:
`test:run DATA_FOLDER N QX`
