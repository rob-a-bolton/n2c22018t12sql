# n2c22018t12sql

## What is this?

This is a tool to import the [n2c2 2018 Track 1](https://portal.dbmi.hms.harvard.edu/projects/n2c2-2018-t1/) "Cohort Selection for Clinical Trials" data into a SQL database.

By default it's capable of using Sqlite3, MariaDB, and Postgres. Adding the jar file for additional drivers to the classpath is possible should you need support for others.

## Usage
The following example shows how to run the tool to import tagged XML files from `/path/to/dir` into a sqlite database called `train.sqlite3`:
```sh
clj -M -m n2c22018t12sql.core -t 'table-name' -a true -d /path/to/dir -j 'jdbc:sqlite:train.sqlite3'
```

The following arguments are supported:
+ *-j*, *--jdbc* **URL** JDBC database connection string (DSN).
+ *-t*, *--table-name* **TABLE-NAME** Name of table in database to insert to.
+ *-d*, *--dataset-dir* **PATH** Path to directory containing n2c2 XML files.
+ *-a*, *--annotated* **TRUE/FALSE** Whether data is annotated with cohort challenge tags (met/not met). Defaults to false.
+ *-?*, *--help* Prints help message

Additionally it is possible to provide *--jdbc* as an environment variable instead of a command line option.
To do this set the **JDBC** environment variable to the DSN you would like to use. This is recommended to prevent leaking your database username/password as command-line arguments of running processes may often be accessible to other users.

Please note that *--dataset-dir* will scan recursively for all XML files. For this reason do not mix unnecessary XML files in with the n2c2 data, even if in subfolders.

## Why

SQL is more readily supported as a data source for a lot of further work, compared to XML.

Specifically this allows for easy conversion to an RDF graph using R2RML.