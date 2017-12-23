#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'movieid'
SORT_COLUMN_NAME_SECOND_TABLE = 'movieid1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'movieid1'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    name = "RangePart"
    cursor = openconnection.cursor()
    rangePartition(InputTable, SortingColumnName, 5, openconnection,'rangepart');
    threads = []
    for i in range(5):
        tablename = name + `i`
        t = threading.Thread(target=ThreadSort, args=(tablename, SortingColumnName, openconnection, ))
        threads.append(t)
        t.start()
        # Wait for all threads to complete
        for t in threads:
            t.join()
        print "Exiting Main Thread"
    createQuery = "CREATE TABLE %s as SELECT * FROM %s with no data"
    cursor.execute(createQuery%(OutputTable,InputTable))
    for i in range(5):
        outTableName = 'rangepart'+`i`+'out'
        cursor.execute("INSERT INTO %s SELECT * FROM %s"%(OutputTable, outTableName))
    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    name1 = 'rangepart_t1_'
    name2 = 'rangepart_t2_'
    rangePartition(InputTable1, Table1JoinColumn, 5, con,'rangepart_t1_');
    rangePartition(InputTable2, Table2JoinColumn, 5, con,'rangepart_t2_');
    cursor = openconnection.cursor()
    threads = []
    for i in range(5):
        tablename1 = name1 + `i`
        tablename2 = name2 + `i`
        t = threading.Thread(target=ThreadJoin, args=(tablename1, tablename2, Table1JoinColumn, Table2JoinColumn, openconnection, ))
        threads.append(t)
        t.start()
        # Wait for all threads to complete
        for t in threads:
            t.join()
        print "Exiting Main Thread"
    createQuery = "CREATE TABLE %s as SELECT * FROM %s with no data"
    cursor.execute(createQuery%(OutputTable,'joinedOut0'))
    writeToOutJoin(OutputTable, openconnection)
    openconnection.commit()

def ThreadSort (InputTable, SortingColumnName, openconnection):
    cursor = openconnection.cursor()
    OutputTable = InputTable + 'out'
    cursor.execute("SELECT * INTO %s FROM %s ORDER BY %s"%(OutputTable, InputTable, SortingColumnName))

def ThreadJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection):
    cursor = openconnection.cursor()
    inTable = InputTable1
    OutputTable = 'joinedOut' + inTable[-1]
    cursor.execute("select * into %s from %s t1 join %s t2 on t1.%s = t2.%s"%(OutputTable, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn))
    openconnection.commit()
def writeToOutJoin(OutputTable, openconnection):
    cursor = openconnection.cursor()
    for i in range(5):
        outTableName = 'joinedOut'+`i`
        cursor.execute("INSERT INTO %s SELECT * FROM %s"%(OutputTable, outTableName))
    openconnection.commit()

def rangePartition(tablename, Col_Name, numberofpartitions, openconnection, name):
    
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" %tablename)
        if not bool(cursor.rowcount):
            print "Please Load the Table first!!!"
            return
        #cursor.execute("CREATE TABLE IF NOT EXISTS RangeTableMetadata(PartitionNum INT, MinValue REAL, MaxValue REAL)")
        cursor.execute("SELECT MIN(%s) FROM %s" %(Col_Name, tablename))
        MinValue = cursor.fetchone()[0]
        #print MinValue
        cursor.execute("SELECT MAX(%s) FROM %s" %(Col_Name, tablename))
        MaxValue = cursor.fetchone()[0]
        step = (MaxValue-MinValue)/(float)(numberofpartitions)
        
        i = 0;
        while MinValue < MaxValue:
            lowerLimit = MinValue
            upperLimit = MinValue + step
            newTableName = name + `i`
            Col_Name_New = tablename + '.' + Col_Name
            # if lowerLimit < 0:
            #     lowerLimit = 0.0
            
            # if lowerLimit == 0.0:
            #     cursor.execute("SELECT * INTO %s FROM %s WHERE %s >= %f AND %s <= %f"%(newTableName, tablename, Col_Name_New, lowerLimit, Col_Name_New, upperLimit))
            
            if lowerLimit != 0.0:
                cursor.execute("SELECT * INTO %s FROM %s WHERE %s >= %f AND %s <= %f"%(newTableName, tablename, Col_Name_New, lowerLimit, Col_Name_New, upperLimit))
            MinValue = upperLimit
            i+=1;
        #cursor.execute("select * into outTable from rangepart0")
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
            print 'Error %s' % e
            sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
            print 'Error %s' % e
            sys.exit(1)

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='Magus4brida', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
