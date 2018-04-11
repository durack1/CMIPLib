
import fx

# define variables to search for
variable_list=['tas']

# Define search directory
parent = '/p/css03/cmip5_css02/data/cmip5/'

# update SQL DB
fx.updateSqlDb(parent, variables=variable_list) # this line updates the db for subsetted variables
# fx.updateSqlDb(parent) # this line updates the db for all variables

# get directories to scan
dirs_to_scan = fx.getScanList(variables=['tas'])



# example updating database with xml write
p='/p/css03/cmip5_css02/data/cmip5/output1/NCAR/CCSM4/historical/mon/atmos/Amon/r1i1p1/v20130425/tas'
xmlFile = 'test.xml'
xmlwrite = '2018-01-01 00:00:00'
error = 'FALSE'
fx.updateSqlCdScan(p, xmlFile, xmlwrite, error)