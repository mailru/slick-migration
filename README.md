Slick-migration
===============

Slick-migration is an open source JVM library that allows developers to manage database schema migrations.  
DB schema migrations are vital for any project that requires a database. 

This library has distinctive features:

* declare migrations in the source files that can be organized with Scala's cake-pattern;
* each migration has a unique URN-identifier;
* migrations can declare explicit dependencies and will be processed in appropriate order (using topological sort);
* support SQL-scripts with PostgreSQL syntax;

Having compilable migrations is very useful when developing features in independent branches. The compiler will ensure that a branch is merged properly to 
the trunk migration graph.

License
-------

The license for the library is MIT.

Links
-----
* source code: https://github.com/mailru/slick-migration
