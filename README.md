# pg_partmaint
Super Simple partition maintenance automation for PostgreSQL

Initial partitoning of the table with proper partitioning can be done manually. This script is not useful for Initial partitioning 
This script requires atleast one partition existing for the partitioned table so that it can understand the boundaries.


EXAMPLES:
  ./pg_partmaint.py -c "host=localhost" -t "public.order" -i weekly -p 5 --displayddl -a "TABLESPACE tblspace1"
  ./pg_partmaint.py -c "host=localhost" -t "public.booking" -i 1000000 -p 5 --displayddl -a "TABLESPACE tblspace1"

