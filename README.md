# pg_partmaint
Super Simple automation for declerative partition maintenance for PostgreSQL

## Features:
1. No metadata repo needed and no extension to be created
2. Automatic detection of Partition boundary
3. Yearly, Quarterly, Monthly, Weekly and Daily partitioning
4. Generation of DDL script to file
5. Optional, direct creation of partitions against the database

## Requirements :
* This script is written in python and require Psycopg2 to be present in the system. Install if required<br>
  For example in CentOS 7 :<br>
  ``` sudo yum install python-psycopg2```
* There should be at least 1 partition which will be used as a reference for generating further partitions

## parameters
-c  
--connection  
Host connection string. for example:  
`-c "host=HostnameOrIP dbname=databasename user=username password=password"`  
host can be socket file location also

-t  
--table  
Name of the partitioned table in schemaname.tablename format. for example:  
`-t "public.order"`  

-i  
--interval  
Interval of the new partitions to be created. for example :  
`-i monthly`

-p  
--premake  
Number of partitions to be premaked. They are advance empty partitions.  For example:  
`-p 5`

-a  
--append  
Any string to be appended to the DDL Statement. good place to specify tablespace info  
`-a "TABLESPACE tblspace1"`

--ddlfile  
Specify a file to write the generated DDLs. example:  
`--ddlfile ddl.sql`

--displayddl  
Print the DDLs to screen.

--execute  
Execute DDLs against database to create new partitions.

## EXAMPLES:

```./pg_partmaint.py -c "host=localhost" -t "public.order" -i weekly -p 5 --displayddl -a "TABLESPACE tblspace1"```

```./pg_partmaint.py -c "host=localhost" -t "public.booking" -i 1000000 -p 5 --displayddl -a "TABLESPACE tblspace1"```
