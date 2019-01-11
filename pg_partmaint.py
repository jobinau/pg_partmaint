#!/usr/bin/python

##########################################################################
# Postgres Partition maintenance Script for native partitioning in PostgreSQL
version = 1.0
# Author : Jobin Augustine
##########################################################################

import sys,datetime,argparse,psycopg2
from psycopg2 import extras

#Global Vars :(
strtTime = datetime.datetime.now()


#Command Line Argument parser and help display
parser = argparse.ArgumentParser(description='Index Analysis and Rebuild Program',
	epilog='Example 1:\n %(prog)s -c "host=host1.hostname.com dbname=databasename user=username password=password"\n'
    'Example 2:\n %(prog)s -c "host=host1.hostname.com dbname=databasename user=username password=password"  --tsvfile=test.tsv --ddlfile=ddl.sql --errorlog=error.log --execute --quitonerror',
	formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-c','--connection',help="Connection string containing host, username, password etc",required=True)
parser.add_argument('-t','--table',help="Table name in schema.tablename format",required=True)
parser.add_argument('-i','--interval',help="Interval in 'yearly' | 'quarterly' | 'monthly' | 'weekly' | 'daily' | 'hourly' ",required=True)
parser.add_argument('-p','--premake',help="Premake partition",required=True)
parser.add_argument('--ddlfile',help="Generate DDL as SQL Script")
parser.add_argument('--errorlog',help="Error log file")
parser.add_argument('--displayddl', action='store_true', help="Display Generated DDLs on the screen")
parser.add_argument('--quitonerror', action='store_true', help="Exit on execution Error")
parser.add_argument('--iratio',help="Minimum Index Ratio. above which it is considered for reindexing",default=0.9)
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
    print("Getting interval");
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
        return 'unknown'

def prepareSQL(sql):
    sql = sql + " AND n.nspname = split_part('" + str(args.table) + "', '.', 1)::name AND c.relname = split_part('" + str(args.table) + "', '.', 2)::name"
    return (sql)

def preformatSQL(sql,oid,colname,coltype):
# SELECT 'CREATE TABLE emp_'||to_char(max + (interval '1 month'*b),'YYYY_MM')||' PARTITION OF emp FOR VALUES FROM ('''||max + (interval '1 month'*b)||''') TO ('''||max + (interval '1 month'*(b+1))||''')' FROM
# (SELECT max(substring(pg_catalog.pg_get_expr(c.relpartbound, c.oid),position('TO (' IN pg_catalog.pg_get_expr(c.relpartbound, c.oid))+5,10)::date)
# FROM pg_catalog.pg_class c join pg_catalog.pg_inherits i on c.oid=i.inhrelid
# WHERE i.inhparent = 16556) a CROSS JOIN generate_series(0,5,1) b;
    interval = getInterVal()

    sql = ("SELECT 'CREATE TABLE " + str(args.table) + "_p'||to_char(max + (interval '" + interval + "'*b),'YYYY_MM')||' PARTITION OF " + str(args.table) +
    " FOR VALUES FROM ('''||max + (interval '" + interval + "'*b)||''') TO ('''||max + (interval '" + interval + "'*(b+1))||''')' FROM " +
    "(SELECT max(substring(pg_catalog.pg_get_expr(c.relpartbound, c.oid),position('TO (' IN pg_catalog.pg_get_expr(c.relpartbound, c.oid))+5,10)::date) " +
    "FROM pg_catalog.pg_class c join pg_catalog.pg_inherits i on c.oid=i.inhrelid " +
    "WHERE i.inhparent = 16556) a CROSS JOIN generate_series(0," + str(args.premake) +",1) b")
    return sql

#Get the Indexes in a Dictionary.
def getIdxDict():
    print("Quering the Database...")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    index_list = cur.fetchall()
    cur.close()
    print("Number of Indexes are: " + str(len(index_list)))
    return index_list

def preparePartitions():
    print("Preparing Partitions...")
    sql = """SELECT c.oid,a.attname, t.typname
    FROM pg_attribute a
    JOIN pg_class c ON a.attrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_type t ON a.atttypid = t.oid
    WHERE attnum IN (SELECT unnest(partattrs) FROM pg_partitioned_table p WHERE a.attrelid = p.partrelid)"""
    cur = conn.cursor()
    cur.execute(prepareSQL(sql))
    attr = cur.fetchone()
    oid = attr[0]
    colname = attr[1]
    coltype = attr[2]
    #print(attr)
    cur.close()
    sql = preformatSQL(sql,oid,colname,coltype)
    print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    attr = cur.fetchone()
    print(attr)
    cur.close();


#generate DDL statements
def genStmnts(index_list):
    print("Generating DDL Statements...")
    for o in index_list:
        #print(o['uks'])
        if o['indisprimary'] == False and o['fks'] == None and o['uks'] ==None  :
            idef = o['inddef']
            dupIndx = idef[0:idef.find('INDEX')+5]+" CONCURRENTLY "+o['index_name']+"_bk"+idef[idef.find('INDEX')+6+len(o['index_name']):]
            #print(dupIndx)
            o['DDL1'] = dupIndx
            dropIdx = "DROP INDEX \"" + o['nspname'] + "\"." + o['index_name']
            #print(dropIdx)
            o['DDL2'] = dropIdx
            renIndx = "ALTER INDEX \"" + o['nspname'] + "\"." + o['index_name'] + "_bk RENAME TO " + o['index_name']
            #print(renIndx)
            o['DDL3'] = renIndx
        elif o['indisprimary'] == True and o['fks'] == None and o['uks'] ==None :
            idef = o['inddef']
            dupIndx = idef[0:idef.find('INDEX')+5]+" CONCURRENTLY "+o['index_name']+"_bk"+idef[idef.find('INDEX')+6+len(o['index_name']):]
            o['DDL1'] = dupIndx
            #print(dupIndx)
            renIndx = "ALTER TABLE \"" + o['nspname'] + "\"." + o['table_name'] + " DROP CONSTRAINT " + o['index_name'] + ", ADD CONSTRAINT " \
                + o['index_name'] + " PRIMARY KEY USING INDEX " + o['index_name'] + "_bk"
            o['DDL2'] = renIndx
        elif o['uks'] == 1 and o['fks'] == None :
            idef = o['inddef']
            dupIndx = idef[0:idef.find('INDEX')+5]+" CONCURRENTLY "+o['index_name']+"_bk"+idef[idef.find('INDEX')+6+len(o['index_name']):]
            o['DDL1'] = dupIndx
            renIndx = "ALTER TABLE \"" + o['nspname'] + "\"." + o['table_name'] + " DROP CONSTRAINT " + o['index_name'] + ", ADD CONSTRAINT " \
                + o['index_name'] + " UNIQUE USING INDEX " + o['index_name'] + "_bk"
            o['DDL2'] = renIndx


#print DDLs to terminal (stdout)
def printDDLs(index_list):
    for o in index_list:
        for i in range(1,len(o)-12):
            print(o['DDL'+str(i)])

def writeDDLfile(index_list,ddlfile):
    fd = open(ddlfile, 'w')
    fd.truncate()
    for o in index_list:
        fd.write("----Rebuiding "+o['index_name']+" on table "+o['nspname'] + "." + o['table_name']+"-----\n")
        for i in range(1,len(o)-12):
            fd.write(o['DDL'+str(i)]+";\n")
        fd.write('\n')
    fd.close()

def writeIndexTSV(index_list,tsvfile):
    print("Generating Tab Seperated File : "+ tsvfile)
    fd1 = open(tsvfile,'w')
    for o in index_list:
        fd1.write(strtTime.strftime('%Y-%m-%d %H:%M:%S')+"\t"+o['nspname']+"."+o['table_name']+"."+o['index_name']+"\t"+str(o['iratio'])+"\t"+str(o['idxsize'])+"\n")
    fd1.close()

def executeDDLs(index_list):
    if args.errorlog:
        fd = open(args.errorlog,'w')
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    for o in index_list:
        for i in range(1,len(o)-12):
            strDDL = o['DDL'+str(i)]
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


    interval = getInterVal()
    if interval == 'unknown':
        print("ERROR : Interval type specified is not correct")
    else:
        print(interval)
    preparePartitions()
    # index_list = getIdxDict()
    # genStmnts(index_list)

    #if user specified the --displayddl option
    if args.displayddl:
        printDDLs(index_list)

    if args.ddlfile:
        writeDDLfile(index_list,args.ddlfile)

    #if user specified the --tsvfile option
    # if args.tsvfile :
    #     writeIndexTSV(index_list,args.tsvfile)

    #if user specified the --execute option
    if args.execute:
        print("Auto execute is Enabled")
        executeDDLs(index_list)
    else:
        print("Auto execute is disabled")

    close_conn(conn)
