Legobase Interpreter
====================

LegoBase is an efficient query engine in Scala programming language. This project
hosts the query interpreter and runtime libraries of LegoBase.

Installation
============

The LegoBase interpreter requires the SC Pardis Library (https://github.com/epfldata/sc) project `sc-pardis-library`.

If you have access to the sc repository, follow these steps:

1. Clone this project and checkout to the desired branch. 

2. Then, after entering sbt interactive mode, run `sc-pardis-library/publish-local`.

sbt takes care of fetching the necessary dependencies. 


Testing
=======
For testing the interpreter, go to the `lego-core` project using `project lego-core` in sbt.
Then, for running query X with scaling factor N, you have to run the following command:
`run DATA_FOLDER N QX`
