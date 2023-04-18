# 4300-Cheetah

## Description
DB Relation Manager project for CPSC4300 at Seattle U, Spring 2023, Project Cheetah.

## Features
### Milestone 1
A SQL parser for ` CREATE TABLE ` and ` SELECT ` SQL statements. Takes statements from the user, validates them, cleans them, and displays them. Uses the [Hyrise SQL parser](https://github.com/klundeen/sql-parser) to convert the user's input.

### Milestone 2
A heap storage engine using [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html). Implements a slotted page structure for blocks (` DbBlock `), heap files (` DbFile `), and heap tables (` DbRelation `). ` HeapTable ` supports ` create `, ` create if not exists `, ` open `, ` close `, ` drop `, ` insert`, and simple ` select ` (no clauses). It also only supports column attributes INT and TEXT.

## Installation
1. Clone the repository on CS1

` git clone https://github.com/BguardiaOpen/4300-Cheetah23SQ.git `

2. Ensure the ` .bash_profile ` path is configured correctly

```
export PATH=/usr/local/db6/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/db6/lib:$LD_LIBRARY_PATH
export PYTHONPATH=/usr/local/db6/lib/site-packages:$PYTHONPATH 
```

## Usage
1. Create a directory to hold the database (first time usage only)
2. Compile the program with ` make `
3. Run the program with ` ./m path_to_database_directory `
    
    * The path must be the path to the directory from the root user@cs1
4. Other ``` make ``` options
    
    * ` make clean `: removes the object code files
    * ` make valgrind `: shows locations of memory leaks (might be necessary to change database directory in ` Makefile `)
5. User input options

    * SQL ` CREATE TABLE ` and ` SELECT ` statements (see example)
    * ` test ` runs the Milestone 2 tests
    * ` quit ` exits the program

## Example

```
$ ./m cpsc4300/data
SQL> create table foo (a text, b integer, c double)
CREATE TABLE foo (a TEXT, b INT, c DOUBLE)
SQL> select * from foo left join goober on foo.x=goober.x
SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x
SQL> select f.a,g.b,h.c from foo as f join goober as g on f.id = g.id where f.z >1
SELECT f.a, g.b, h.c FROM foo AS f JOIN goober AS g ON f.id = g.id WHERE f.z > 1
SQL> not real sql
Invalid SQL: not real sql
SQL> quit
```

## Acknowledgements
* [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html)
* [Berkeley DB Dbt](https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/frame_main.html)
* [Hyrise SQL Parser](https://github.com/klundeen/sql-parser)
* [Professor Lundeen's 5300-Instructor base code](https://github.com/klundeen/5300-Instructor/releases/tag/Milestone2h) 🙏🙏
* [The Python equivalent](https://github.com/BguardiaOpen/cpsc4300py)