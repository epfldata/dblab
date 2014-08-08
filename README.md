Legobase
======

A Cost-based optimizing compiler for DSLs

Installation
============

TODO

Clone these project and checkout to the mentioned branch. Then, run `sbt publish-local`.

Testing
=======
For testing shallow version, go to `lego-core` project using `project lego-core`.
Then for running query X with scaling factor N you have to run the following command:
`run DATA_FOLDER N QX`

For testing deep version, first you have to generate the code. 
For that purpose you have to go to `legocompiler` project using `project legocompiler`.
Then for generating query X with scaling factor N you have to run the following command:
`run DATA_FOLDER N QX`
Then for testing the correctness you have copy the generated file into `test` folder of `legocompiler` project.
Then you have to run the following command:
`test:run DATA_FOLDER N QX`
