#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Documentation for CMIPLib:
-----
Created on Wed Apr 11 10:59:24 2018

Paul J. Durack and Stephen Po-Chedley 11th April 2018

|  PJD 11 Apr 2018  - Initialized repository
|  SP  15 Jun 2018  - Added initial functions for reading/writing SQL DB

@author: durack1
"""

# import numpy as np
import datetime
import MySQLdb # pip install mysqlclient
import sys
sys.path.append('config/')
import scandir
import sql_cmip5_config as sqlcfg
import numpy as np

def scantree(path):
    """Recursively yield directories with 1) no subdirectories and 2) files.

    https://stackoverflow.com/questions/33135038/how-do-i-use-os-scandir-to-return-direntry-objects-recursively-on-a-directory
    """    
    for entry in scandir.walk(path):
        if ((entry[1] == []) & (entry[2] != [])):
            yield entry[0] + '/'
    

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

def process_path(path):
    ''' 
    process_path(path):

    function takes a path and infers the metadata (e.g., variable, version, model, etc.)

    returns: validPath, variable, version, realization, cmipTable, realm, tfreq, experiment, model, institute, product

    Note that validPath is a boolean indicating whether the path is valid
    '''        
    meta = path.split('/')[1:-1]
    validPath = True
    e = path.split('/')[-2]
    if ((e == 'bad0') | (e == 'bad1')):
        meta = meta[0:-1]
    if (len(meta) > 10):
        if ((meta[-1] != '1') & (meta[-1] != '2')):
            variable = meta[-1]
            version = meta[-2]
            realization = meta[-3]
            cmipTable = meta[-4]
            realm = meta[-5]
            tfreq = meta[-6]
            experiment = meta[-7]
            model = meta[-8]
            institute = meta[-9]
            product = meta[-10]
        else:
            variable = meta[-2]
            version = meta[-1]
            realization = meta[-3]
            cmipTable = meta[-4]
            realm = meta[-5]
            tfreq = meta[-6]
            experiment = meta[-7]
            model = meta[-8]
            institute = meta[-9]
            product = meta[-10]
    else:
        validPath = False
        variable = []
        version = []
        realization = []
        cmipTable = []
        realm = []
        tfreq = []
        experiment = []
        model = []
        institute = []
        product = []
    return validPath, variable, version, realization, cmipTable, realm, tfreq, experiment, model, institute, product

# def updateSqlDb(path):
#     ''' 
#     updateSqlDb(parent, keyword_arguments)

#     function takes in a parent directory (e.g., /path/to/cmipData/output1/) and will parse 
#     all available data and update an underlying MySQL database with data path information. 

#     Functionality depends on specific directory structure:
#         /product/institute/model/experiment/time_frequency/realm/cmip_table/realization/version/variable
#         or
#         /product/institute/model/experiment/time_frequency/realm/cmip_table/realization/variable/version

#     keyword_arguments: 
#         Keyword     Type    Purpose
#         variables   list    Specify list of variables to parse
#     '''    
#     # create database connection
#     conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
#     c = conn.cursor()

#     # get existing paths
#     print('Getting existing directories under: ' + path)
#     query = 'select * from paths where path like \'' + path + '%\';'
#     c.execute(query)
#     a = c.fetchall()    
#     a = np.array(a)
#     if len(a) > 0:
#         pexist = list(a[:,1])
#         modTime = list(a[:,13])
#         pathLookup = dict(zip(pexist, modTime))
#         del a, pexist, modTime, query
#     else:
#         pathLookup = {'',''}

#     # lists for previously undocumented directories
#     new_paths = []
#     new_ctimes = []
#     new_mtimes = []
#     new_atimes = []
#     # lists for directories with updated timestamps
#     update_paths = []
#     update_ctimes = []
#     update_mtimes = []
#     update_atimes = []

#     # get scan iterator
#     x = scantree(path)
#     # iterate over directories to see if they need to be stored
#     last = ''
#     for i, d in enumerate(x):
#         # get file path
#         file_path = d.path.split(d.name)[0]
#         # check if the directory is unique in iterator
#         if file_path != last:
#             last = file_path
#             # check if directory is in database
#             # else check time stamp and add to list if it is new
#             if file_path not in pathLookup:
#                 try:
#                     new_paths.append(file_path)
#                     new_ctimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(d.stat().st_ctime)))
#                     new_mtimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(d.stat().st_mtime)))
#                     new_atimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(d.stat().st_atime)))
#                 except:
#                     print('Error accessing ' + file_path)
#             else:
#                 oldModTime = str(pathLookup[file_path])
#                 oldModTime = strToDatetime(oldModTime)+datetime.timedelta(seconds=1) # add 1s to account for ms precision on file system
#                 newModTime = datetime.datetime.utcfromtimestamp(d.stat().st_mtime)
#                 if newModTime > oldModTime:
#                     update_paths.append(file_path)
#                     update_ctimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(d.stat().st_ctime)))
#                     update_mtimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(d.stat().st_mtime)))
#                     update_atimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(d.stat().st_atime)))

#     print('Found ' + str(len(update_paths) + len(new_paths)) + ' directories')
#     # write out new paths to database
#     x = list(zip(new_paths,new_mtimes,new_ctimes,new_atimes))
#     outputList = []
#     for path, mtime, ctime, atime in x:
#         validPath, variable, version, realization, cmipTable, realm, tfreq, experiment, model, institute, product = process_path(path)
#         if validPath:
#             litem = [path, product, institute, model, experiment, tfreq, realm, cmipTable, realization, version, variable, ctime, mtime, atime, '0', None]     
#             outputList.append(litem)
#     q = """ INSERT INTO paths (
#             path, product, institute, model, experiment, tfreq, realm, cmipTable, realization, version, variable, created, modified, accessed, retired, retire_datetime) 
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#         """
#     if len(outputList) > 0:
#         c.executemany(q, outputList)
#         conn.commit()

#     print('Wrote out ' + str(len(outputList)) + ' new directories to DB')

#     # write out modified paths to database
#     x = list(zip(update_paths,update_mtimes,update_ctimes,update_atimes))
#     outputList = []
#     for path, mtime, ctime, atime in x:
#         litem = [ctime, mtime, atime, '0', None, path]
#         outputList.append(litem)
#     q = """ UPDATE paths 
#             SET created=%s, modified=%s, accessed=%s, retired=%s, retire_datetime=%s
#             WHERE path=%s;
#         """
#     if len(outputList) > 0:
#         c.executemany(q, outputList)
#         conn.commit()

#     print('Updated ' + str(len(outputList)) + ' directories in DB')

#     # close db connection
#     conn.close()

def updateSqlDb(path):
    ''' 
    updateSqlDb(parent, keyword_arguments)

    function takes in a parent directory (e.g., /path/to/cmipData/output1/) and will parse 
    all available data and update an underlying MySQL database with data path information. 

    Functionality depends on specific directory structure:
        /product/institute/model/experiment/time_frequency/realm/cmip_table/realization/version/variable
        or
        /product/institute/model/experiment/time_frequency/realm/cmip_table/realization/variable/version

    keyword_arguments: 
        Keyword     Type    Purpose
        variables   list    Specify list of variables to parse
    '''    
    # create database connection
    conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
    c = conn.cursor()

    # get existing paths
    print('Getting existing directories under: ' + path)
    query = 'select * from paths where path like \'' + path + '%\';'
    c.execute(query)
    a = c.fetchall()    
    a = np.array(a)
    if len(a) > 0:
        pexist = list(a[:,1])
        modTime = list(a[:,13])
        pathLookup = dict(zip(pexist, modTime))
        del a, pexist, modTime, query
    else:
        pathLookup = {'',''}

    # lists for previously undocumented directories
    new_paths = []
    new_ctimes = []
    new_mtimes = []
    new_atimes = []
    # lists for directories with updated timestamps
    update_paths = []
    update_ctimes = []
    update_mtimes = []
    update_atimes = []

    # get scan iterator
    x = scantree(path)
    # iterate over directories to see if they need to be stored
    for i, file_path in enumerate(x):
        # check if directory is in database
        # else check time stamp and add to list if it is new
        if file_path not in pathLookup:
            try:
                new_paths.append(file_path)
                ts = scandir.stat(file_path)
                new_ctimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(ts.st_ctime)))
                new_mtimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(ts.st_mtime)))
                new_atimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(ts.st_atime)))
            except:
                print('Error accessing ' + file_path)
        else:
            oldModTime = str(pathLookup[file_path])
            oldModTime = strToDatetime(oldModTime)+datetime.timedelta(seconds=1) # add 1s to account for ms precision on file system
            ts = scandir.stat(file_path)
            newModTime = datetime.datetime.utcfromtimestamp(ts.st_mtime)
            if newModTime > oldModTime:
                update_paths.append(file_path)
                update_ctimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(ts.st_ctime)))
                update_mtimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(ts.st_mtime)))
                update_atimes.append(toSQLtime(datetime.datetime.utcfromtimestamp(ts.st_atime)))

    print('Found ' + str(len(update_paths) + len(new_paths)) + ' new/modified directories')
    # write out new paths to database
    x = list(zip(new_paths,new_mtimes,new_ctimes,new_atimes))
    outputList = []
    for path, mtime, ctime, atime in x:
        validPath, variable, version, realization, cmipTable, realm, tfreq, experiment, model, institute, product = process_path(path)
        if validPath:
            litem = [path, product, institute, model, experiment, tfreq, realm, cmipTable, realization, version, variable, ctime, mtime, atime, '0', None]     
            outputList.append(litem)
    q = """ INSERT INTO paths (
            path, product, institute, model, experiment, tfreq, realm, cmipTable, realization, version, variable, created, modified, accessed, retired, retire_datetime) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    if len(outputList) > 0:
        c.executemany(q, outputList)
        conn.commit()

    print('Wrote out ' + str(len(outputList)) + ' new directories to DB')

    # write out modified paths to database
    x = list(zip(update_paths,update_mtimes,update_ctimes,update_atimes))
    outputList = []
    for path, mtime, ctime, atime in x:
        litem = [ctime, mtime, atime, '0', None, path]
        outputList.append(litem)
    q = """ UPDATE paths 
            SET created=%s, modified=%s, accessed=%s, retired=%s, retire_datetime=%s
            WHERE path=%s;
        """
    if len(outputList) > 0:
        c.executemany(q, outputList)
        conn.commit()

    print('Updated ' + str(len(outputList)) + ' directories in DB')

    # close db connection
    conn.close()    

def getScanList(**kwargs):
    ''' 
    getScanList()

    function returns a list of directories that: 
        + do not have an xml file and no cdscanerror or cdscanerror='FALSE'
        + have a path that is modified

    keyword_arguments: 
        Keyword     Type    Purpose
        variables   list    Specify list of variables to parse  

    ''' 
    # create database connection
    conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
    c = conn.cursor()   

    # handle keyword arguments
    if 'variables' in kwargs:
        variable_subset_list = kwargs['variables']
        if type(variable_subset_list)==str:
            variable_subset_list = [variable_subset_list]
        variable_subset = True
    else:
        variable_subset = False

    if variable_subset:
        paths = []
        for var in variable_subset_list:
            query = 'select * from paths where ((xmlFile IS NULL and ((cdscanerror IS NULL) OR (cdscanerror = \'FALSE\'))) OR ((xmlwritedatetime < modified))) and variable=\'' + var + '\'' + ' and retired=0;'  
            c.execute(query)
            a = c.fetchall()    
            a = np.array(a)
            if a.shape[0] > 0:
                paths.extend(list(a[:,1]))
    else:
        query = 'select * from paths where ((xmlFile IS NULL and cdscanerror IS NULL) OR ((xmlwritedatetime < modified)));'
        c.execute(query)
        a = c.fetchall()    
        a = np.array(a)
        paths = list(a[:,1])
    conn.commit()
    conn.close()
    return paths

def updateSqlCdScan(full_path, xmlFile, xmlwriteDatetime, error):
    ''' 
    updateSqlCdScan(full_path, xmlFile, xmlwrite, error)

    function updates the database for a given full_path updating: 
        + xmlFile - xml file name (and path)
        + xmlwriteDatetime - write timestamp formatted as YYYY-MM-DD HH:MM:SS
        + error - 

    keyword_arguments: 
        Keyword     Type    Purpose
        variables   list    Specify list of variables to parse  

    ''' 
    # create database connection
    conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
    c = conn.cursor()   
    query = 'update paths set xmlFile = \'{FILE}\', xmlwritedatetime = \'{WRITEDATE}\', cdscanerror=\'{ERROR}\' where path = \'{PATH}\';'.format(FILE=xmlFile, WRITEDATE=xmlwriteDatetime, ERROR=error, PATH=full_path)
    c.execute(query)
    conn.commit()
    conn.close()

def retireDirectory(full_path):
    ''' 
    retireDirectory(full_path)

    function retires a directory from the database: 
        + xmlFile - xml file name (and path)

    ''' 
    retire_datetime = toSQLtime(datetime.datetime.now())

    # create database connection
    conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
    c = conn.cursor()   
    query = 'update paths set retired=1, retire_datetime = \'' + retire_datetime + '\' where path = \'' + full_path + '\';'
    c.execute(query)
    conn.commit()
    conn.close()









