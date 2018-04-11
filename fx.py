
# Import dependencies
import os
import glob
import datetime
import MySQLdb # pip install mysqlclient
import sys
sys.path.append('config/')
import sql_cmip5_config as sqlcfg
import time
import scandir # conda install scandir
import numpy as np


# Define helper functions
def toSQLtime(time):
	'''
	helper function takes datetime object and returns a string date to insert into SQL database
	'''		
	time = str(time.year) + '-' + str(time.month) + '-' + str(time.day) + ' ' + str(time.hour) + ':' + str(time.minute) + ':' + str(time.second) 
	return time

def strToDatetime(time):
	'''
	helper function takes SQL string datetime format and converts it to a datetime object
	'''	
	t = time.split('-')
	year = int(t[0])
	month = int(t[1])
	day = int(t[2].split(' ')[0])
	hour = int(t[2].split(' ')[1].split(':')[0])
	minute = int(t[2].split(' ')[1].split(':')[1])
	second = int(t[2].split(' ')[1].split(':')[2])
	time = datetime.datetime(year,month,day,hour,minute,second)
	return time	

def update_path(current_path, subdir):
	'''
	helper function removes extra slashes in path (e.g., /path/to//data/ -> /path/to/data)
	'''
	path_out = current_path + '/' + subdir
	path_out = path_out.replace('//','/')
	return path_out

def get_path_set(current_path):
	'''
	helper function to get set of sub paths for a given directory
	'''
	try:
		path_set = scandir.listdir(current_path)
	except OSError:
		print('Not a valid path : ' + current_path)
		path_set = []
	return path_set

def updateSqlDb(parent, **kwargs):
	''' 
	updateSqlDb(parent, keyword_arguments)

	function takes in a parent directory (e.g., /path/to/cmipData/) which has products as subdirectories
	(e.g., /path/to/cmipData/output1/) and will parse all available data and update an underlying MySQL 
	database with data path information. 

	Functionality depends on specific directory structure:
		/product/institute/model/experiment/time_frequency/realm/cmip_table/realization/version/variable

	keyword_arguments: 
		Keyword 	Type	Purpose
		variables 	list 	Specify list of variables to parse
	'''


	# check if we should only scan for some variables
	if 'variables' in kwargs:
		variable_subset_list = kwargs['variables']
		variable_subset = True
	else:
		variable_subset = False

	# create database connection
	conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
	c = conn.cursor()

	# get existing paths
	query = 'select * from paths;'
	c.execute(query)
	a = c.fetchall()	
	a = np.array(a)
	if len(a) > 0:
		pexist = list(a[:,1])
		modTime = list(a[:,12])
		pathLookup = dict(zip(pexist, modTime))
		del a, pexist, modTime, query
	else:
		pathLookup = {'',''}

	# Get initial set of products
	products = scandir.listdir(parent)

	# Timing information
	start = time.time()

	# Loop through products, institutes, models, experiments, time frequencies, realms, cmip tables, realizations, versions, and variables
	# For each end-directory get the date created/modified
	# Update DB as needed for each path
	for product in products:
		product_path = parent + '/' + product
		institutes = get_path_set(product_path)
		for institute in institutes:
			institute_path = update_path(product_path, institute)
			models = get_path_set(institute_path)
			print(institute)
			for model in models:
				model_path = update_path(institute_path, model)
				experiments = get_path_set(model_path)
				for experiment in experiments:
					tfreqs_path = update_path(model_path, experiment)
					tfreqs = get_path_set(tfreqs_path)				
					for tfreq in tfreqs:
						realms_path = update_path(tfreqs_path, tfreq)
						realms = get_path_set(realms_path)
						for realm in realms:
							cmipTables_path = update_path(realms_path, realm)
							cmipTables = get_path_set(cmipTables_path)
							for cmipTable in cmipTables:
								realizations_path = update_path(cmipTables_path, cmipTable)
								realizations = get_path_set(realizations_path)
								for realization in realizations:
									versions_path = update_path(realizations_path, realization)
									versions = get_path_set(versions_path)
									for version in versions:
										variables_path = update_path(versions_path, version)
										variables = get_path_set(variables_path)
										for variable in variables:
											if variable_subset:
												if variable in variable_subset_list:
													file_path = update_path(variables_path, variable)
													dstat = scandir.stat(file_path)
													mod_time = toSQLtime(datetime.datetime.utcfromtimestamp(dstat.st_mtime))
													create_time = toSQLtime(datetime.datetime.utcfromtimestamp(dstat.st_ctime))
													access_time = toSQLtime(datetime.datetime.utcfromtimestamp(dstat.st_atime))
													if file_path in pathLookup:
														oldModTime = str(pathLookup[file_path])
														oldModTime = strToDatetime(oldModTime)
														newModTime = strToDatetime(mod_time)
														if oldModTime != newModTime:
															# update created/modified times
															c.execute('UPDATE paths SET created = \'{C}\', modified = \'{M}\', accessed = \'{A}\' WHERE path = \'{PATH}\';'.format(PATH=file_path, C=create_time, M=mod_time, A=access_time))
													else:
														# Add path information to SQL DB
														c.execute('INSERT INTO paths (path, product, institute, model, experiment, tfreq, cmipTable, realization, version, variable, created, modified, accessed) VALUES (\'{PATH}\', \'{PRODUCT}\', \'{INSTITUTE}\', \'{MODEL}\', \'{EXPERIMENT}\', \'{TFREQ}\', \'{CMIPTABLE}\', \'{REALIZATION}\', \'{VERSION}\', \'{VARIABLE}\', \'{C}\', \'{M}\', \'{A}\');'.format(PATH=file_path, PRODUCT=product, INSTITUTE=institute, MODEL=model, EXPERIMENT=experiment, TFREQ=tfreq, CMIPTABLE=cmipTable, REALIZATION=realization, VERSION=version, VARIABLE=variable, C=create_time, M=mod_time, A=access_time))
												else:
													# variable isn't in subsetted list - go on without processing
													continue
											else:
												file_path = update_path(variables_path, variable)
												dstat = scandir.stat(file_path)
												mod_time = toSQLtime(datetime.datetime.utcfromtimestamp(dstat.st_mtime))
												create_time = toSQLtime(datetime.datetime.utcfromtimestamp(dstat.st_ctime))
												access_time = toSQLtime(datetime.datetime.utcfromtimestamp(dstat.st_atime))
												if file_path in pathLookup:
													oldModTime = str(pathLookup[file_path])
													oldModTime = strToDatetime(oldModTime)
													newModTime = strToDatetime(mod_time)
													if oldModTime != newModTime:
														# update created/modified times
														c.execute('UPDATE paths SET created = \'{C}\', modified = \'{M}\', accessed = \'{A}\' WHERE path = \'{PATH}\';'.format(PATH=file_path, C=create_time, M=mod_time, A=access_time))
												else:
													# Add path information to SQL DB
													c.execute('INSERT INTO paths (path, product, institute, model, experiment, tfreq, cmipTable, realization, version, variable, created, modified, accessed) VALUES (\'{PATH}\', \'{PRODUCT}\', \'{INSTITUTE}\', \'{MODEL}\', \'{EXPERIMENT}\', \'{TFREQ}\', \'{CMIPTABLE}\', \'{REALIZATION}\', \'{VERSION}\', \'{VARIABLE}\', \'{C}\', \'{M}\', \'{A}\');'.format(PATH=file_path, PRODUCT=product, INSTITUTE=institute, MODEL=model, EXPERIMENT=experiment, TFREQ=tfreq, CMIPTABLE=cmipTable, REALIZATION=realization, VERSION=version, VARIABLE=variable, C=create_time, M=mod_time, A=access_time))												
									# Commit SQL DB updates for each model for each dataset version
									conn.commit()

	# Close db connection									
	conn.close()
	# print time
	end = time.time()
	print(end - start)

def getScanList(**kwargs):
	''' 
	getScanList()

	function returns a list of directories that: 
		+ do not have an xml file and no cdscanerror or cdscanerror='FALSE'
		+ have a path that is modified

	keyword_arguments: 
		Keyword 	Type	Purpose
		variables 	list 	Specify list of variables to parse	

	'''	
	# create database connection
	conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
	c = conn.cursor()	

	# handle keyword arguments
	if 'variables' in kwargs:
		variable_subset_list = kwargs['variables']
		variable_subset = True
	else:
		variable_subset = False

	if variable_subset:
		paths = []
		for var in variable_subset_list:
			query = 'select * from paths where ((xmlFile IS NULL and ((cdscanerror IS NULL) OR (cdscanerror = \'FALSE\'))) OR ((xmlwritedate < modified))) and variable=\'' + var + '\'' + ';'	
			c.execute(query)
			a = c.fetchall()	
			a = np.array(a)
			if a.shape[0] > 0:
				paths.extend(list(a[:,1]))
	else:
		query = 'select * from paths where ((xmlFile IS NULL and cdscanerror IS NULL) OR ((xmlwritedate < modified)));'
		c.execute(query)
		a = c.fetchall()	
		a = np.array(a)
		paths = list(a[:,1])
	conn.commit()
	conn.close()
	return paths

def updateSqlCdScan(full_path, xmlFile, xmlwrite, error):
	''' 
	updateSqlCdScan(full_path, xmlFile, xmlwrite, error)

	function updates the database for a given full_path updating: 
		+ xmlFile - xml file name (and path)
		+ xmlwrite - write timestamp formatted as YYYY-MM-DD HH:MM:SS
		+ error - 

	keyword_arguments: 
		Keyword 	Type	Purpose
		variables 	list 	Specify list of variables to parse	

	'''	
	# create database connection
	conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
	c = conn.cursor()	
	query = 'update paths set xmlFile = \'{FILE}\', xmlwritedate = \'{WRITEDATE}\', cdscanerror=\'{ERROR}\' where path = \'{PATH}\';'.format(FILE=xmlFile, WRITEDATE=xmlwrite, ERROR=error, PATH=full_path)
	c.execute(query)
	conn.commit()
	conn.close()



