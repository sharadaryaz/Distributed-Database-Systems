#!/opt/local/bin/python
import psycopg2
import math

DATABASE_NAME = 'postgres'
TABLE_NAME = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
meta_tablename = 'meta_table'
UserId = 'userid'
movieid = 'movieid'
rating = 'rating'
INPUT_FILE_PATH = 'graderData.dat'

def getopenconnection(user='postgres', password='Magus4brida', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")




def create_table(ptablename, openconnection):
	cur = openconnection.cursor()
	cur.execute('DROP TABLE IF EXISTS {0}'.format(ptablename))
	cur.execute("""
         CREATE TABLE IF NOT EXISTS {0}(userid INTEGER,
          movieid INTEGER,
          rating REAL
         );""".format(ptablename))



def insert_in_table_range(ptablename, TABLE_NAME, low_part, high_part, openconnection):
	cur = openconnection.cursor()
	cur.execute("""INSERT INTO {0} (userid, movieid, rating) (
          SELECT userid, movieid, rating
          FROM {1}
          WHERE rating > {2} AND rating <= {3}
        );""".format(ptablename, TABLE_NAME, low_part, high_part))


def insert_in_table_rr(ptablename, partition_ranges, openconnection):
	cur = openconnection.cursor()
	cur.execute("""INSERT INTO {0} (userid, movieid, rating) (
          SELECT userid, movieid, rating
          FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER () rn FROM {1}) sl
          WHERE rn IN {2}
        );""".format(ptablename, TABLE_NAME, partition_ranges))

def insert(ptablename, data, openconnection):
	cur = openconnection.cursor()
	x = data[0]
	y = data[1]
	z = data[2]
  	cur.execute('INSERT INTO {0} (userid, movieid, rating) VALUES ({1}, {2}, {3})'.format(ptablename, x, y, z))


def loadratings(ratingstablename, ratingsfilepath, openconnection):
	cur = openconnection.cursor()
	cur.execute('DROP TABLE IF EXISTS {0}'.format(ratingstablename))

	cur.execute("CREATE TABLE {0} (UserID integer, c1 char, MovieID integer, c2 char, Rating real, c3 char, xdata int);".format(ratingstablename))
	
	
	with open(INPUT_FILE_PATH) as f:
		cur.copy_from(f, TABLE_NAME, sep=':', columns=('userid', 'c1', 'movieid', 'c2', 'rating', 'c3', 'xdata'))
	cur.execute("ALTER TABLE {0} DROP COLUMN c1; ALTER TABLE {0} DROP COLUMN c2; ALTER TABLE {0} DROP COLUMN c3; ALTER TABLE {0} DROP COLUMN xdata;".format(ratingstablename))
	# cur.execute("select * from NRatingz;")
	
	# x = cur.fetchall()
	# print x,


def rangepartition(ratingstablename, numberofpartitions, openconnection):
	n = numberofpartitions
	if not isinstance(n, int) or n < 0:
		print 'No a valid number of partition'
		return
    
 	conn = openconnection
 	cur = conn.cursor()

	cur.execute("""INSERT INTO {0} (x, y) VALUES ('a', {1}) ;""".format(meta_tablename, numberofpartitions))

	part_num = 0
	low_part = 0.0
	diff = (5.0/numberofpartitions)

	high_part =  low_part + diff

	while part_num<numberofpartitions:
		partition_name = 'range_part{0}'.format(part_num)
		create_table(partition_name, openconnection)
		
		#Inserting rest of the ratings
		insert_in_table_range(partition_name, TABLE_NAME, low_part, high_part, openconnection)
		high_part +=diff
		low_part += diff
		part_num += 1
	#Inserting if rating is 0
	insert_in_table_range(partition_name, TABLE_NAME, -0.5, 0, openconnection)

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
	n = numberofpartitions
	if not isinstance(n, int) or n < 0:
		print 'No a valid number of partition'
		return
	conn = openconnection
	cur = conn.cursor()

	cur.execute("""INSERT INTO {0} (x, y) VALUES ('b', {1}) ;""".format(meta_tablename, numberofpartitions))


	#Finding total number of rows in master table
	cur.execute('SELECT COUNT(*) FROM {0};'.format(TABLE_NAME))
	num_of_rows = cur.fetchone()[0]


	for i in range(0,numberofpartitions):
		partition_ranges = ()
		for j in range(1,num_of_rows+1):
			if j%numberofpartitions == i:
				partition_ranges = partition_ranges + (j,)
		partition_name = 'rrobin_part{0}'.format(i)
		create_table(partition_name, openconnection)
		insert_in_table_rr(partition_name, partition_ranges, openconnection)


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
	cur = openconnection.cursor()
	cur.execute('''SELECT Y FROM {0} WHERE X='b'; '''.format(meta_tablename))
	numberofpartitions = int(cur.fetchone()[0])
	cur.execute('SELECT COUNT(*) FROM {0};'.format(TABLE_NAME))
	num_of_rows = cur.fetchone()[0]
	i = num_of_rows%numberofpartitions 
	partition_name = 'rrobin_part{0}'.format(i)
	insert(partition_name, [userid, itemid, rating], openconnection)


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
	cur = openconnection.cursor()
	cur.execute('''SELECT Y FROM {0} WHERE X='a'; '''.format(meta_tablename))
	numberofpartitions = int(cur.fetchone()[0])
	p_size = 5.0/numberofpartitions
	p_num = max(int(math.ceil(rating / p_size)), 1)
	print p_num
	partition_name = 'range_part{0}'.format(p_num)
	insert(partition_name, [userid, itemid, rating], openconnection)

def deletepartitionsandexit(openconnection):
	cur = openconnection.cursor()
	cur.execute('''SELECT Y FROM {0} WHERE X='a'; '''.format(meta_tablename))

	no_of_range_partition = int(cur.fetchone()[0])

	cur.execute('''SELECT Y FROM {0} WHERE X='b'; '''.format(meta_tablename))

	no_of_rr_partition = int(cur.fetchone()[0])

	for i in range(0, no_of_range_partition):
		cur.execute('DROP TABLE IF EXISTS {0}{1}'.format(RANGE_TABLE_PREFIX, i))
	for i in range(0, no_of_rr_partition):
		cur.execute('DROP TABLE IF EXISTS {0}{1}'.format(RROBIN_TABLE_PREFIX, i))
	print ('Deleted all the partitions')

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname)
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to

	

	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

	cur = conn.cursor()
	
	cur.execute('DROP TABLE IF EXISTS {0}'.format(meta_tablename))

	cur.execute("CREATE TABLE {0} (x  char, y integer);".format(meta_tablename))


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

	cur.execute("CREATE TABLE {0} (UserID integer, c1 char, MovieID integer, c2 char, Rating real, c3 char, xdata int);".format(TABLE_NAME))
        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

       # create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as conn:
        	
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(conn, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings(TABLE_NAME,INPUT_FILE_PATH, conn)

			
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(conn, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail