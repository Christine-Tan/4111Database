
Homework01 by Xinyue Tan(xt2215)

Code:
    there are comments in the code to help others understand.

Data
    For DataTables, I used PeopleSmall.csv for test.
    (The file contains less rows than original one. It will be more convenient to display.)
    For the TopTenHitters, used Batting.csv and People.csv for test

The function of each file lists as following:

-- CSVDataTable.py
    init CSVDataTable class by passing (logical name, name of csv file, name of primary key(s)).
    functions: load/save/print/insert/delete the csv file
    Tips: MUST call the load() function to read the data into in-memory space before doing other things.
    Correctness Rules:
	1. find_by_primary_key(): number of values should be equal to the number of key_columns
				  the result is unique
	2. insert():		  input row must contain the value of primary key
				  the value of primary key shouldn’t exist in the original table
				  keys in the template are part of the header of csv file	
	3. find_by_template():	  keys in the template are part of the header of csv file
	4. delete():		  calls find_by_template method	

-- RDBDataTable.py
    name of the db: baseball
    init CSVDataTable class by passing (name of database, name of table, name of primary key(s)).
    functions: load/save/print/insert/delete tables in db

-- KeyError
    duplicate primary key / lack of primary key / keys are not existed

-- ValueError
    incompatible number of input values with number of primary key

-- TopTenHittersCSV.py
    calculate the top ten hitters in csv file

-- TopTenHittersRDB.py
    calculate the top ten hitters in db
    I only applied the constraints of yearID > 1960.

-- Snapshot
    the snapshot of running tests

If you have any further questions please send me an email (xt2215@columbia.edu)