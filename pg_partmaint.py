#!/usr/bin/python

##########################################################################
# Postgres Partition maintenance Script for native partitioning in PostgreSQL
version = 2.0
# Author : Jobin Augustine
##########################################################################

import sys,datetime,argparse,psycopg2
from psycopg2 import extras

#Global Vars :(
strtTime = datetime.datetime.now()


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

def getInterVal():
    #print("Getting interval");
    inInterval = args.interval
    if inInterval == 'yearly':
        return '1 year'
    elif inInterval == 'quarterly':
        return '3 months'
    elif inInterval == 'monthly':
        return '1 month'
    elif inInterval == 'weekly':
        return '1 week'
    elif inInterval == 'daily':
        return '1 day'
    elif inInterval == 'hourly':
        return '1 hour'
    else:
        return inInterval

def prepareSQL(sql):
    sql = sql + " AND n.nspname = split_part('" + str(args.table) + "', '.', 1)::name AND c.relname = split_part('" + str(args.table) + "', '.', 2)::name"
    return (sql)

def preformatSQL(sql,oid,colname,coltype):
    interval = getInterVal()
    if interval.isdigit():
      sql = ("SELECT 'CREATE TABLE " + str(args.table) + "_p'|| max + " + interval + "*b ||' PARTITION OF " + str(args.table) +
      " FOR VALUES FROM ('''||max + "+ interval +" * b ||''') TO ('''||max + " + interval + " *(b+1)||''')' AS ddl FROM " +
      "(SELECT max(left(substring(pg_catalog.pg_get_expr(c.relpartbound, c.oid),position('TO (' IN pg_catalog.pg_get_expr(c.relpartbound, c.oid))+5),-2)::bigint) " +
      "FROM pg_catalog.pg_class c join pg_catalog.pg_inherits i on c.oid=i.inhrelid WHERE i.inhparent = " + str(oid) +") a CROSS JOIN generate_series(0," + str(args.premake) +",1) b")
    else:
      sql = ("SELECT 'CREATE TABLE " + str(args.table) + "_p'||to_char(max + (interval '" + interval + "'*b),'YYYY_MM')||' PARTITION OF " + str(args.table) +
      " FOR VALUES FROM ('''||max + (interval '" + interval + "'*b)||''') TO ('''||max + (interval '" + interval + "'*(b+1))||''')' AS ddl FROM " +
      "(SELECT max(substring(pg_catalog.pg_get_expr(c.relpartbound, c.oid),position('TO (' IN pg_catalog.pg_get_expr(c.relpartbound, c.oid))+5,10)::date) " +
      "FROM pg_catalog.pg_class c join pg_catalog.pg_inherits i on c.oid=i.inhrelid " +
      "WHERE i.inhparent = " + str(oid) +") a CROSS JOIN generate_series(0," + str(args.premake) +",1) b")
    return sql

#Prepare a Dictionary of DDLs for Partitioning.
def preparePartitions():
    print("Preparing Partitions...")
    #Check whether table is partitioned and get details.
    sql = """SELECT c.oid,a.attname, t.typname
    FROM pg_attribute a
    JOIN pg_class c ON a.attrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_type t ON a.atttypid = t.oid
    WHERE attnum IN (SELECT unnest(partattrs) FROM pg_partitioned_table p WHERE a.attrelid = p.partrelid)"""
    cur = conn.cursor()
    sql = prepareSQL(sql)
    #print(sql)
    cur.execute(sql)
    if cur.rowcount < 1 :
       print("ERROR : Unable to locate a partitioned table \"" + str(args.table) + "\"")
       sys.exit()
    attr = cur.fetchone()
    #print(attr)
    cur.close()
	#attr[0] = oid of table, attr[1] = column name, attr[2] = column type
	##Check whether there is sufficient emtpy partitions are there
    sql = ("SELECT count(*) FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, pg_stat_user_tables s " +
    	"WHERE c.oid=i.inhrelid AND i.inhparent = '" + str(attr[0]) +  "' and c.oid = s.relid and s.n_live_tup = 0 ")
    cur = conn.cursor()
    cur.execute(sql)
    parts = cur.fetchone()
    cur.close()
    print('Requested Number of Advanced Empty Partitions : ' + args.premake)
    print('Current number of Empty Partitions : ' + str(parts[0]))
    if parts[0] > int(args.premake) :
        print("NOTICE : Already there are sufficient empty partitions")
        sys.exit(1)

    sql = preformatSQL(sql,attr[0],attr[1],attr[2])
    #print(sql)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    dicDDLs = cur.fetchall()
    cur.close()
    return dicDDLs


#print DDLs to terminal (stdout)
def printDDLs(dicDDLs):
    for o in dicDDLs:
		print(o['ddl']+';')

def writeDDLfile(dicDDLs,ddlfile):
    fd = open(ddlfile, 'w')
    fd.truncate()
    for o in dicDDLs:
        fd.write(o['ddl']+";\n")
    fd.close()

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

	#Prepare a dictionry of all the DDLs required for adding partitoins
    dicDDLs = preparePartitions()

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
