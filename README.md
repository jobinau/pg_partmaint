# pg_partmaint
Super Simple partition maintenance automation for PostgreSQL

## IMPORTANT :

Initial partitoning of the table with proper partitioning can be done manually.
This script requires atleast one partition existing for the partitioned table so that it can understand the boundaries.

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
