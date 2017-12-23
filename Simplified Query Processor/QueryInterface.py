#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    if os.path.exists('RangeQueryOut.txt'):
    	os.remove('RangeQueryOut.txt')
    f = open('RangeQueryOut.txt','w')
    nameRange = "RangeRatings"
    nameRR = "RoundRobinRatings"
    metaname = "MetaData"
    #RangeTableData = selectAllData(openconnection, nameRange+metaname)
    NumberOfRangePartitions = selectCount(openconnection, nameRange+metaname)
    NumberOfRRPartitions = selectCount(openconnection, nameRR+metaname,1)
    #print RangeTableData
    #print NumberOfPartitions
    if ratingMaxValue<ratingMinValue:
    	print 'Value ratingMaxValue should be greater than ratingMinValue'
    	return
    for i in range(0,NumberOfRangePartitions):
    	tableName = nameRange+'Part'+str(i)
    	RatingData = selectRangeData(openconnection, tableName, ratingMinValue, ratingMaxValue)
    	if RatingData:
    		for t in range(len(RatingData)):
    			FileString = tableName + ','
    			FileString += ','.join(str(x) for x in RatingData[t])+'\n'
    			f.write(FileString)
    for i in range(0,NumberOfRRPartitions):
    	tableName = nameRR+'Part'+str(i)
    	RatingData = selectRangeData(openconnection, tableName, ratingMinValue, ratingMaxValue)
    	if RatingData:
    		for t in range(len(RatingData)):
    			FileString = tableName + ','
    			FileString += ','.join(str(x) for x in RatingData[t])+'\n'
    			f.write(FileString)
    f.close()
    if not os.path.getsize('RangeQueryOut.txt') > 0:
    	print "None of the tables contained the specified range."
   #Remove this once you are done with implementation

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    if os.path.exists('PointQueryOut.txt'):
    	os.remove('PointQueryOut.txt')    
    f = open('PointQueryOut.txt','w')
    nameRange = "RangeRatings"
    nameRR = "RoundRobinRatings"
    metaname = "MetaData"
    #RangeTableData = selectAllData(openconnection, nameRange+metaname)
    NumberOfRangePartitions = selectCount(openconnection, nameRange+metaname)
    NumberOfRRPartitions = selectCount(openconnection, nameRR+metaname,1)

    #print RangeTableData
    #print NumberOfPartitions
    for i in range(0,NumberOfRangePartitions):
    	tableName = nameRange+'Part'+str(i)
    	RatingData = selectPointData(openconnection, tableName, ratingValue)
    	if RatingData:
    		for t in range(len(RatingData)):
    			FileString = tableName + ','
    			FileString += ','.join(str(x) for x in RatingData[t])+'\n'
    			f.write(FileString)
    for i in range(0,NumberOfRRPartitions):
    	tableName = nameRR+'Part'+str(i)
    	RatingData = selectPointData(openconnection, tableName, ratingValue)
    	if RatingData:
    		for t in range(len(RatingData)):
    			FileString = tableName + ','
    			FileString += ','.join(str(x) for x in RatingData[t])+'\n'
    			f.write(FileString)    
    f.close()
    if not os.path.getsize('PointQueryOut.txt') > 0:
    	print "None of the tables contained the specified point."

def selectAllData(conn, table):
	cursor = conn.cursor()
	cursor.execute('SELECT * FROM {0};'.format(table))
	#print table
	return cursor.fetchall()


def selectRangeData(conn, table, ratingMinValue, ratingMaxValue):
	cursor = conn.cursor()
	rating = 'rating'
	cursor.execute('SELECT * FROM {0} where {1}>={2} and {1}<={3};'.format(table, rating, ratingMinValue, ratingMaxValue))
	#print table
	return cursor.fetchall()

def selectPointData(conn, table, ratingValue):
	cursor = conn.cursor()
	rating = 'rating'
	cursor.execute('SELECT * FROM {0} where {1}={2};'.format(table, rating, ratingValue))
	#print table
	return cursor.fetchall()


def selectCount(conn, table, flag=0):
	cursor = conn.cursor()
	pn = 'partitionnum'
	if flag == 0:
		cursor.execute('SELECT COUNT(*) FROM {0};'.format(table))
	else:
		cursor.execute('SELECT {0} FROM {1}'.format(pn, table))
	#print table
	return cursor.fetchone()[0]