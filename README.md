DBLAB
======

DBLAB is a framework for building database systems by high-level programming, 
and getting really good performance nevertheless. This is achieved by a new DSL
compiler framework, SC (the Systems Compiler), currently in an advanced
stage of development at the EPFL DATA Lab.

While DBLAB is publicly available for inspection here, SC isn't.
However, we have made a binary release of SC to be able to compile DBLAB.

DBLAB currently only contains the implementation of a single-core main-memory 
analytical database engine. While the names in the code base suggest that
this is LegoBase (published at VLDB 2014), the code in this repository 
is an entirely new development based on the lessons learned from VLDB 2014,
sharing no common code with the previous system.

The roadmap for DBLAB includes the open-source release of a second DBMS, a 
main-memory OLTP system, in the summer of 2016. Subsequently to this, we will
start converging DBLAB towards our vision of a framework and component library
for quickly creating new high-performance database systems and a testbed for
experimenting with new database technology.
