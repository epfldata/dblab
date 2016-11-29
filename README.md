DBLAB
======
[![Build Status](https://travis-ci.org/epfldata/dblab.svg?branch=develop)](https://travis-ci.org/epfldata/dblab)

DBLAB is a framework for building database systems by high-level programming, 
and getting really good performance nevertheless. This is achieved by a new DSL
compiler framework, SC (the Systems Compiler), currently in an advanced
stage of development at the EPFL DATA Lab.

While DBLAB is publicly available for inspection here, SC isn't.
However, we have made a binary release of SC to be able to compile DBLAB.

Project Structure
-----------------
[DBLAB Components](components)
hosts a library of the components of a database system such as parsers, query engine, storage manager, and etc.

[DBLAB Components Compiler](components-compiler)
hosts the compilation-related code for optimizing a database system such as transformers and 
code generators.

[Experimentation](experimentation)
host the experimentation infrasturcture (e.g. the scripts, configurations, and the implementation of some benchmarks) used in the DBLAB systems.

[Systems](systems)
hosts the code base of the systems implemented using the DBLAB Components.

[LegoBase](systems/legobase) 
hosts the implementation of a single-core main-memory 
analytical database engine. While the names in the code base suggest that
this is LegoBase (published at VLDB 2014), the code in this repository 
is an entirely new development based on the lessons learned from VLDB 2014,
sharing no common code with the previous system. To be more concrete,
this implementation only contains a pipeline of the transformers 
existing in the [DBLAB Components Compiler](components-compiler) project.

