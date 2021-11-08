# n2c22018t12sql

## What is this?

This is a tool to import the [n2c2 2018 Track 1](https://portal.dbmi.hms.harvard.edu/projects/n2c2-2018-t1/) "Cohort Selection for Clinical Trials" data into a SQL database.

By default it's capable of using Sqlite3, MariaDB, and Postgres. Adding the jar file for additional drivers to the classpath is possible should you need support for others.

## Usage
The following example shows how to run the tool to import tagged XML files from */path/to/dir* into a sqlite database called *train.sqlite3*:
```sh
clj -M -m n2c22018t12sql.core -t 'table-name' -a true -d /path/to/dir -j 'jdbc:sqlite:train.sqlite3'
```

The following arguments are supported:

+ *-j*, *--jdbc* **URL**: JDBC database connection string.
+ *-p*, *--patient-table* **TABLE-NAME**: Name of patient table. Default: "patients".
+ *-t*, *--doc-table* **TABLE-NAME**: Name of document table. Default: "documents".
+ *-T*, *--annotation-table* **TABLE-NAME**: Table to insert cohort annotations to. Default: "annotations".
+ *-a*, *--annotated*: Whether data is annotated with cohort challenge tags (met/not met). Defaults to false.
+ *-k*, *--keep*: If flag provided then do not drop existing tables before trying to insert data.
+ *-d*, *--dataset-dir* **DIRECTORY**: Path to directory containing n2c2 XML files.
+ *-?*, *--help*: Prints help message.

Additionally it is possible to provide *--jdbc* as an environment variable instead of a command line option.
To do this set the **JDBC** environment variable to the [connection string](https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html) you would like to use. This is recommended to prevent leaking your database username/password as command-line arguments of running processes may often be accessible to other users.

Examples of supported JDBC strings:
* *jdbc:sqlite:C:/Users/me/MyDatabase.db*
* *jdbc:mariadb://localhost:3306/MyDatabase?user=maria&password=hunter2*
* *jdbc:postgresql://localhost/n2c2?user=n2c2&password=n2c2*

Bear in mind that the question mark *?* and ampersand *&* characters are commonly special characters in shells and must be escaped.
For this reason it is recommended you put your connection string in quotes as shown in the example at the top of this README.

Please note that *--dataset-dir* will scan recursively for all XML files. For this reason do not mix unnecessary XML files in with the n2c2 data, even if in subfolders.

## Why

SQL is more readily supported as a data source for a lot of further work, compared to XML.

Specifically this allows for easy conversion to an RDF graph using R2RML.