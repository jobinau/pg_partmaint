#!/usr/bin/python

##########################################################################
# Postgres Partition maintenance Script for native partitioning in PostgreSQL
version = 3.2
# Author : Jobin Augustine
##########################################################################

import sys,datetime,argparse,psycopg2
from psycopg2 import extras

#Command Line Argument parser and help display
parser = argparse.ArgumentParser(description='Index Analysis and Rebuild Program',
	epilog='Example 1:\n %(prog)s -c "host=host1.hostname.com dbname=databasename user=username password=password" -t public.emp -i weekly -p 5 \n'
    'Example 2:\n %(prog)s -c "host=host1.hostname.com dbname=databasename user=username password=password" -t public.emp -i weekly -p 5 --tsvfile=test.tsv --ddlfile=ddl.sql --errorlog=error.log --execute --quitonerror',
	formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-c','--connection',help="Connection string containing host, username, password etc",required=True)
parser.add_argument('-t','--table',help="Table name in schema.tablename format",required=True)
parser.add_argument('-i','--interval',help="Interval in [ yearly | quarterly | monthly | weekly | daily | hourly | <NUMBER> ]",required=True)
parser.add_argument('-p','--premake',help="Premake partition",required=True)
parser.add_argument('-a','--append',help="Special string to append to DDL")
parser.add_argument('--ddlfile',help="Generate DDL as SQL Script")
parser.add_argument('--errorlog',help="Error log file")
parser.add_argument('--displayddl', action='store_true', help="Display Generated DDLs on the screen")
parser.add_argument('--quitonerror', action='store_true', help="Exit on execution Error")
parser.add_argument('--execute', action='store_true',help="Execute the generated DDLs against database")
if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()

#Print the version of this program to stdout
def print_version():
    print("Version: "+str(version))

#Establish connection to database and handle exception
def create_conn():
    print("Connecting to Databse...")
    try:
       conn = psycopg2.connect(args.connection+" connect_timeout=5")
    except psycopg2.Error as e:
       print("Unable to connect to database :")
       print(e)
       sys.exit(1)
    return conn

#close the connection
def close_conn(conn):
    print("Closing the connection...")
    conn.close()

############################## Class representing a Partitioned table ######################################
class PartTable:
    'Class representing the a Paritioned table' #this is __doc__
    def __init__(self,name):
        self.name = name
        #Query to identify the partitioning column and its type
        sql= """SELECT c.oid,a.attname, t.typname
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_type t ON a.atttypid = t.oid
            WHERE attnum IN (SELECT unnest(partattrs) FROM pg_partitioned_table p WHERE a.attrelid = p.partrelid)""" + \
            " AND n.nspname = split_part('" + str(args.table) + "', '.', 1)::name AND c.relname = split_part('" + str(args.table) + "', '.', 2)::name"
        
        #print('########## find the partition key ######\n'+sql+'\n###########################')
        cur = conn.cursor()
        cur.execute(sql)
        if cur.rowcount < 1 :
            print("ERROR : No partitioned table with name :\"" + str(args.table) + "\"")
            sys.exit()
        #print('Verified that table : ' + self.name + ' is a partitioned table')
        self.attr = cur.fetchone()
        #attr[0] = oid of table, attr[1] = column name, attr[2] = column type
        cur.close()
        inInterval = args.interval
        if inInterval == 'yearly':
            self.interval = '1 year'
            self.partFormat = 'YYYY'
        elif inInterval == 'quarterly':
            self.interval = '3 months'
            self.partFormat = 'YYYY_MM'
        elif inInterval == 'monthly':
            self.interval = '1 month'
            self.partFormat = 'YYYY_MM'
        elif inInterval == 'weekly':
            self.interval = '1 week'
            self.partFormat = 'YYYY_MM_DD'
        elif inInterval == 'daily':
            self.interval = '1 day'
            self.partFormat = 'YYYY_MM_DD'
        elif inInterval == 'hourly':
            self.interval = '1 hour'
            self.partFormat = 'YYYY_MM_DD_HH24'
        else:
            self.interval = inInterval


    def getFreePartCount(self):             ## Get the number of empty partitions using the oid of the parent.
        #sql = ("SELECT count(*) FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_stat_user_tables s " +
    	#"WHERE c.oid=i.inhrelid AND i.inhparent = '" + str(self.attr[0]) +  "' and c.oid = s.relid and s.n_live_tup = 0 ")
        sql=" SELECT COUNT(*) FROM pg_catalog.pg_inherits i JOIN pg_stat_user_tables s ON i.inhrelid = s.relid \
            WHERE i.inhparent = '" + str(self.attr[0]) +  "' AND s.n_live_tup = 0"
        #print('########## No. of empty partitions ######\n'+sql+'\n###########################')
        cur = conn.cursor()
        cur.execute(sql)
        parts = cur.fetchone()
        cur.close()
        return parts[0]

    def prepareNewPartitions(self,newPartCount):        ##Prepare DDLs for 'newPartCount' number of new partitions for the table
        print('Preparing '+ str(newPartCount) + ' more new partition(s)')
        if self.interval.isdigit():
            sql = ("SELECT 'CREATE TABLE " + str(args.table) + "_p'|| max + " + self.interval + "*b ||' PARTITION OF " + str(args.table) +
            " FOR VALUES FROM ('''||max + "+ self.interval +" * b ||''') TO ('''||max + " + self.interval + " *(b+1)||''')' AS ddl FROM " +
            "(SELECT max(left(substring(pg_catalog.pg_get_expr(c.relpartbound, c.oid),position('TO (' IN pg_catalog.pg_get_expr(c.relpartbound, c.oid))+5),-2)::bigint) " +
            "FROM pg_catalog.pg_class c join pg_catalog.pg_inherits i on c.oid=i.inhrelid "+ 
            "WHERE i.inhparent = " + str(self.attr[0]) +" AND pg_catalog.pg_get_expr(c.relpartbound, c.oid) != 'DEFAULT') a CROSS JOIN generate_series(0," + str(newPartCount-1) +",1) b")
        else:
            #Addressed 1 and 2 objectives from TODO items
            sql = ("SELECT 'CREATE TABLE " + str(args.table) + "_p'||to_char(max + (interval '" + self.interval + "'*b),'"+ self.partFormat +"')||' PARTITION OF " + str(args.table) +
            " FOR VALUES FROM ('''||max + (interval '" + self.interval + "'*b)||''') TO ('''||max + (interval '" + self.interval + "'*(b+1))||''')' AS ddl FROM " +
            "(SELECT max(left(substring(pg_catalog.pg_get_expr(c.relpartbound, c.oid),position('TO (' IN pg_catalog.pg_get_expr(c.relpartbound, c.oid))+5),-2)::timestamp) " +
            "FROM pg_catalog.pg_class c join pg_catalog.pg_inherits i on c.oid=i.inhrelid " +
            "WHERE i.inhparent = " + str(self.attr[0]) +" AND pg_catalog.pg_get_expr(c.relpartbound, c.oid) != 'DEFAULT') a CROSS JOIN generate_series(0," + str(newPartCount-1) +",1) b")
        print('########## prepare DDLs ######\n'+sql+'\n###########################')
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        if cur.rowcount < 1 :
            print("ERROR : Atleast one partiton should be existing which marks the begining of Partitions for table : \"" + str(args.table) + "\"")
            sys.exit()

        self.dicDDLs = cur.fetchall()
        cur.close()

    def getNewPartDDLs(self):           ##Get the Dictionary object which contains all the new partition definisions.
        if len(self.dicDDLs) < 0:
            print("No DDLs for New Partitions")
            sys.exit()
        return self.dicDDLs

############################# End of PartTable Class #################################################################

#Generic function : print DDLs to terminal (stdout)
def printDDLs(dicDDLs):
    for o in dicDDLs:
		print(o['ddl']+';')

#Generic functoin : print DDLs to a file
def writeDDLfile(dicDDLs,ddlfile):
    fd = open(ddlfile, 'w')
    fd.truncate()
    for o in dicDDLs:
        fd.write(o['ddl']+";\n")
    fd.close()

#Generic function : Execute DDLs against database
def executeDDLs(dicDDLs):
    if args.errorlog:
        fd = open(args.errorlog,'w')
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    for o in dicDDLs:
        strDDL = o['ddl']
        try:
            cur = conn.cursor()
            print("Executing :" + strDDL)
            cur.execute(strDDL)
            conn.commit()
            cur.close()
        except psycopg2.Error as e:
            print("Statement Execution Error :")
            print(e)
            if args.errorlog:
                fd.write(strDDL + str(e))
            if args.quitonerror :
                sys.exit(1)
    conn.set_isolation_level(old_isolation_level)
    if args.errorlog:
        fd.close()

#main() function of the program
if __name__ == "__main__":
    print_version()
    conn = create_conn()

    tab1 = PartTable(args.table)
    freeParts = tab1.getFreePartCount()
    
    print('Current Number of Free Partitions in the table :'+ str(freeParts) )
    if freeParts >= int(args.premake) :
        print("NOTICE : Already there are sufficient empty partitions")
        sys.exit(1)
	
    tab1.prepareNewPartitions(int(args.premake)-freeParts)
    #Prepare a dictionry of all the DDLs required for adding partitoins
    #dicDDLs = preparePartitions()
    dicDDLs = tab1.getNewPartDDLs()
    
    #append special string to DDLs
    if args.append:
        for o in dicDDLs:
           o['ddl'] = o['ddl'] + ' ' + args.append
    
    #if user specified the --displayddl option
    if args.displayddl:
        printDDLs(dicDDLs)

    if args.ddlfile:
        writeDDLfile(dicDDLs,args.ddlfile)

    #if user specified the --execute option
    if args.execute:
        print("Auto execute is Enabled")
        executeDDLs(dicDDLs)
    else:
        print("Auto execute is disabled")

    close_conn(conn)
