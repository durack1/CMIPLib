#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Documentation for CMIPLib:
-----
Created on Wed Apr 11 10:59:24 2018

Paul J. Durack and Stephen Po-Chedley 11th April 2018

|  PJD 11 Apr 2018  - Initialized repository
|  SP  15 Jun 2018  - Added initial functions for reading/writing SQL DB
|  PJD 19 Jun 2018  - Updated with functions from make_cmip5_xml1.py
|  PJD 19 2018       - Updated to use conda mysqlclient

## conda create -n cdat8 -c conda-forge -c cdat python=3.6 cdat mesalib joblib mysql-connector-python scandir

@authors: durack1, @pochedley1
"""

from __future__ import print_function ; # Python2->3 conversion
import datetime
import gc
import mysql.connector as MySQLdb #pip install mysqlclient; or conda install mysql-connector-python or conda install mysql-python + pip install mysql-connector-python-rf (py2) or (cdat80py2) bash-4.1$ conda install -c carta mysqlclient
import numpy as np
import os
import re
import sys
sys.path.append('config/')
import scandir
import sql_cmip5_config as sqlcfg
import time
import errno
from glob import glob
from subprocess import Popen,PIPE
import json

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

def createGridLabel(mip_era, realm, cmipTable, grid, dimensions):
    # get realm id
    realmIdLookup = {'aerosol' : 'ae', 'atmos' : 'ap', 'atmosChem' : 'ac', 
                    'land' : 'ld', 'landIce' : 'gi', 'seaIce' : 'si', 
                    'ocean' : 'op', 'ocnBgchem' : 'oc', 'river' : 'rr'}
    realmId = realmIdLookup['atmos']
    
    # vert-id lookup information
    z1List = set(['height2m', 'height10m', 'depth0m', 'depth100m', 'olayer100m', 'sdepth1', 'sdepth10'])
    lList = set(['olevel', 'olevhalf', 'alevel', 'alevhalf'])
    reP = re.compile(r'p[0-9]')
    pCheck = [not not re.search(reP,i) for i in dimensions]
    rePl = re.compile(r'pl[0-9]')
    plCheck = [not not re.search(rePl,i) for i in dimensions]    
    rePlev = re.compile(r'plev[0-9]')
    plevCheck = [not not re.search(rePlev,i) for i in dimensions]

    # get vert id
    if len(z1List.intersection(set(dimensions))) > 0:
        vertId = 'z1'
    elif (any(pCheck) | any(plCheck)):
        vertId = 'p1'
    elif len(lList.intersection(set(dimensions))) > 0:
        vertId = 'l'        
    elif 'alev1'in dimensions:
        vertId = 'l1'
    elif 'sdepth'in dimensions:
        vertId = 'z'
    elif 'alt16'in dimensions:
        vertId = 'z16'        
    elif 'alt40'in dimensions:
        vertId = 'z40'
    elif 'rho'in dimensions:
        vertId = 'd'      
    elif any(plevCheck):  
        dimensions = np.array(dimensions)
        vertId = 'p' + dimensions[plevCheck][0].split('plev')[1]
    else:
        vertId = '2d'

    # get region id
    if mip_era == 'CMIP6':
        if cmipTable in ['IfxAnt', 'IyrAnt', 'ImonAnt']:
            regionId = 'ant'
        elif cmipTable in ['IfxGrn', 'IyrGrn', 'ImonGrn']:
            regionId = 'grn'
        else:
            regionId = 'glb'
    else:
        regionId = 'glb'

    # get h1 variable
    locList = set(['site', 'oline', 'basin', 'siline'])
    dimList = set(['latitude', 'yant', 'ygre', 'longitude', 'xant', 'yant'])    
    if len(locList.intersection(set(dimensions))) > 0:
        h1 = 's'
    elif cmipTable in ('AERmonZ', 'E6hrZ', 'EdayZ', 'EmonZ'):
        h1 = 'z'
    elif len(dimList.intersection(set(dimensions))) == 0:
        h1 = 'm'
    else:
        h1 = 'g'

    gridLabel = regionId + '-' + realmId + '-' + vertId + '-' + h1

    if mip_era == 'CMIP6':
        if grid == 'gm':
            gridLabel = gridLabel + 'n'
        else:
            gridStrip = grid.replace('g','').replace('a','').replace('z','')
            gridLabel  = gridLabel + gridStrip

    return gridLabel

   
def produceCMIP5Activity(experiment):
    activityTable = {'sst2030' : 'CFMIP', 'sstClim' : 'CFMIP', 'sstClim4xCO2' : 'CFMIP', 
                    'sstClimAerosol' : 'CFMIP', 'sstClimSulfate' : 'CFMIP', 
                    'amip4xCO2' : 'CFMIP', 'amipFuture' : 'CFMIP', 'aquaControl' : 'CFMIP', 
                    'aqua4xCO2' : 'CFMIP', 'aqua4K' : 'CFMIP', 'amip4K' : 'CFMIP', 
                    'piControl' : 'CMIP', 'historical' : 'CMIP', 'esmControl' : 'CMIP',
                    'esmHistorical' : 'CMIP', '1pctCO2' : 'CMIP', 'abrupt4xCO2' : 'CMIP', 
                    'amip' : 'CMIP', 'historicalExt' : 'CMIP', 'esmrcp85' : 'C4MIP', 
                    'esmFixClim1' : 'C4MIP', 'esmFixClim2' : 'C4MIP', 'esmFdbk1' : 'C4MIP', 
                    'esmFdbk2' : 'C4MIP', 'historicalNat' : 'DAMIP', 'historicalGHG' : 'DAMIP', 
                    'historicalMisc' : 'DAMIP', 'midHolocene' : 'PMIP', 'lgm' : 'PMIP', 
                    'past1000' : 'PMIP', 'rcp45' : 'ScenarioMIP', 'rcp85' : 'ScenarioMIP', 
                    'rcp26' : 'ScenarioMIP', 'rcp60' : 'ScenarioMIP'}

    reDec = re.compile(r'decadal[0-9]{4}')
    if not not re.search(reDec, experiment):
        activity = 'DCPP'
    elif experiment in activityTable.keys():
        activity = activityTable[experiment]
    else:
        activity = 'CMIP5'

    return activity



def lookupCMIP6Metadata(cmipTable, variable):
    # https://github.com/PCMDI/cmip6-cmor-tables
    fn = 'cmip6-cmor-tables/Tables/CMIP6_' + cmipTable + '.json'
    with open(fn) as f:
        data = json.load(f)    
    frequency = data['variable_entry'][variable]['frequency']
    realm = data['variable_entry'][variable]['modeling_realm'].split(' ')[0]
    dimensions = data['variable_entry'][variable]['dimensions'].split(' ')
    return frequency, realm, dimensions


def parsePath(path):
    '''
    parsePath(path):

    function takes a path and infers the metadata (e.g., variable, version, model, etc.)

    returns: validPath, variable, version, realization, cmipTable, realm, tfreq, experiment, model, institute, product

    Note that validPath is a boolean indicating whether the path is valid
    '''
    meta = path.split('/')[1:-1]   
    
    validPath = True
    # remove double versions
    if meta[-2] == meta[-3]:
        meta.pop(-2)
    # check for 'bad' directories
    e = path.split('/')[-2]
    bad = re.compile('bad[0-9]{1}')
    check = re.match(bad, e)
    checkBad = True
    if check != None:
        checkBad = False
    if ((len(meta) > 10) & (checkBad)):
        if meta[-10] == 'CMIP6':
            version = meta[-1]
            grid = meta[-2]
            variable = meta[-3]
            cmipTable = meta[-4]
            member = meta[-5] 
            experiment = meta[-6]
            model = meta[-7]
            institute = meta[-8]
            activity = meta[-9] 
            mip_era = meta[-10] 
            frequency, realm, dimensions = lookupCMIP6Metadata(cmipTable, variable)
            # gridLabel = createGridLabel(mip_era, realm, cmipTable, grid, dimensions)
        elif ((meta[-1] != '1') & (meta[-1] != '2')):
            variable = meta[-1]
            version = meta[-2]
            member = meta[-3]
            cmipTable = meta[-4]
            realm = meta[-5]
            frequency = meta[-6]            
            experiment = meta[-7]
            model = meta[-8]
            institute = meta[-9]
            activity = produceCMIP5Activity(experiment)
            mip_era = 'CMIP5'
            grid = 'gx'
            # frequencyx, realmx, dimensions = lookupCMIP6Metadata(cmipTable, variable)
            # gridLabel = createGridLabel(mip_era, realm, cmipTable, grid, dimensions)
        else:
            variable = meta[-2]
            version = meta[-1]
            member = meta[-3]
            cmipTable = meta[-4]
            realm = meta[-5]
            frequency = meta[-6]            
            experiment = meta[-7]
            model = meta[-8]
            institute = meta[-9]
            activity = produceCMIP5Activity(experiment)
            mip_era = 'CMIP5'
            grid = 'gx'
            # frequencyx, realmx, dimensions = lookupCMIP6Metadata(cmipTable, variable)
            # gridLabel = createGridLabel(mip_era, realm, cmipTable, grid, dimensions)
    else:
        validPath = False
        version = []
        grid = []
        variable = []
        cmipTable = []
        realm = []
        frequency = []
        # gridLabel = []
        member = []
        experiment = []
        model = []
        institute = []
        activity = []
        mip_era = []

    return validPath, variable, version, member, cmipTable, realm, frequency, grid, experiment, model, institute, activity, mip_era


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
    query = 'select path, modified from paths where path like \'' + path + '%\';'
    c.execute(query)
    a = c.fetchall()
    a = np.array(a)
    if len(a) > 0:
        pexist = list(a[:,0])
        modTime = list(a[:,1])
        pathLookup = dict(zip(pexist, modTime))
        del a, pexist, modTime, query
    else:
        pathLookup = {'',''}
    query = 'select path from invalid_paths;'
    c.execute(query)
    a = c.fetchall()
    a = np.array(a)
    if len(a) > 0:
        invalidPaths = list(a[:,0])
        del a
    else:
        invalidPaths = []

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
        if ((file_path not in pathLookup) & (file_path not in invalidPaths)):
            try:
                new_paths.append(file_path)
                ts = scandir.stat(file_path)
                new_ctimes.append(toSQLtime(datetime.datetime.fromtimestamp(ts.st_ctime)))
                new_mtimes.append(toSQLtime(datetime.datetime.fromtimestamp(ts.st_mtime)))
                new_atimes.append(toSQLtime(datetime.datetime.fromtimestamp(ts.st_atime)))
            except OSError:
                print('Error accessing ' + file_path)
        elif file_path in pathLookup:
            oldModTime = str(pathLookup[file_path])
            oldModTime = strToDatetime(oldModTime)+datetime.timedelta(seconds=1) # add 1s to account for ms precision on file system
            ts = scandir.stat(file_path)
            newModTime = datetime.datetime.fromtimestamp(ts.st_mtime)
            if newModTime > oldModTime:
                update_paths.append(file_path)
                update_ctimes.append(toSQLtime(datetime.datetime.fromtimestamp(ts.st_ctime)))
                update_mtimes.append(toSQLtime(datetime.datetime.fromtimestamp(ts.st_mtime)))
                update_atimes.append(toSQLtime(datetime.datetime.fromtimestamp(ts.st_atime)))

    print('Found ' + str(len(new_paths)) + ' new directories')
    print('Found ' + str(len(update_paths)) + ' modified directories')
    # write out new paths to database
    x = list(zip(new_paths,new_mtimes,new_ctimes,new_atimes))
    outputList = []
    invalidList = []
    for path, mtime, ctime, atime in x:
        validPath, variable, version, member, cmipTable, realm, frequency, grid, experiment, model, institute, activity, mip_era = parsePath(path)
        if validPath:
            litem = [path, mip_era, activity, institute, model, experiment, cmipTable, realm, frequency, member, grid, version, variable, ctime, mtime, atime, '0', None]
            outputList.append(litem)
        else:
            pathTime = toSQLtime(datetime.datetime.now())
            invalidList.append([path, pathTime])
    q = """ INSERT INTO paths (
            path, mip_era, activity, institute, model, experiment, cmipTable, realm, frequency, member, grid, version, variable, created, modified, accessed, retired, retire_datetime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    if len(outputList) > 0:
        # write out in chunks of 1000
        if len(outputList) > 1000:
            for i in range(int(np.ceil(len(outputList)/1000))):
                ro = outputList[i*1000:i*1000+1000]
                c.executemany(q, ro)
                conn.commit()      
        else:  
            c.executemany(q, outputList)
            conn.commit()    
    # write out invalid paths  
    q = """ INSERT INTO invalid_paths (path, datetime)
            VALUES (%s, %s);
        """
    if len(invalidList) > 0:
        # write out in chunks of 1000
        if len(invalidList) > 1000:
            for i in range(int(np.ceil(len(invalidList)/1000))):
                ro = invalidList[i*1000:i*1000+1000]
                c.executemany(q, ro)
                conn.commit()      
        else:  
            c.executemany(q, invalidList)
            conn.commit()        

    print('Ignored ' + str(len(invalidList)) + ' bad directories')
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
        # write out in chunks of 1000
        if len(outputList) > 1000:
            for i in range(int(np.ceil(len(outputList)/1000))):
                ro = outputList[i*1000:i*1000+1000]
                c.executemany(q, ro)
                conn.commit()      
        else:  
            c.executemany(q, outputList)
            conn.commit()      

    print('Updated ' + str(len(outputList)) + ' directories in DB')

    # close db connection
    conn.close()

def xmlWriteDev(inpath, outfile):
    cmd = 'cdscan -x ' + outfile + ' ' + inpath + '/*.nc'
    cmd = cmd.replace('//', '/')
    p = Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE)
    out,err = p.communicate() 
    # subprocess.call(args, *, stdin=None, stdout=None, stderr=None, shell=False)
    return out, err  


def findInList(keyString, list):
    """find_in_list(keyString, list)

    Intended to subset file lists for keywords. Pass in a list of
    file names (list) and a string (keyString) and the function 
    returns a subsetted list. 

    Example: 

    # Get a list of xmls for a particular model (CCSM4)
    model_list = find_in_list('CCSM4', xml_list)

    """
    outList = [s for s in list if keyString in s]
    return outList; 

def process_path(xmlOutputDir, inpath):
    # parse path
    validPath, variable, version, member, cmipTable, realm, frequency, grid, experiment, model, institute, activity, mip_era = parsePath(inpath)
    reDec = re.compile(r'decadal[0-9]{4}')
    if not not re.search(reDec, experiment):
        experimentPath = 'decadal'
    else:
        experimentPath = experiment
    if validPath:
        # output filename
        # if tfreq == 'fx':
        #     outfile = xmlOutputDir + '/fx/' + variable + '/' + mip_era + '.' + activity + '.' + model + '.' + experiment + '.' + member + '.' + cmipTable + '.' + variable + '.0000000.' + version + '.0.xml'
        # else:    
            # outfile = xmlOutputDir + '/' + experiment + '/' + cmipTable + '/' + variable + '/' + mip_era + '.' + activity + '.' + model + '.' + experiment + '.' + member + '.' + cmipTable + '.' + variable + '.0000000.' + version + '.0.xml'
        outfile = xmlOutputDir + '/' + mip_era + '/' + experimentPath + '/' + realm + '/' + frequency + '/' + variable + '/' + mip_era + '.' + activity + '.' + experiment + '.' + institute + '.' + model + '.' + member + '.' + frequency + '.' + variable + '.' + grid + '.' + version + '.0000000.0.xml'
        outfile = outfile.replace('//','/')
        # check if written already
        ef = glob(outfile.replace('.0000000.','.*.').replace('.0.xml','.*.xml'))
        if len(ef) == 0:
            ensure_dir(outfile)
            # create xml
            out,err = xmlWriteDev(inpath, outfile)
            # parse errors
            xmlwrite = toSQLtime(datetime.datetime.now())
            # check for zero sized files
            zeroSize = False
            if not os.path.exists(outfile):
                fiter = glob(inpath + '/*.nc')
                for fn in fiter:
                    fsize = os.path.getsize(fn)
                    if fsize == 0:
                        zeroSize = True
                        break
                if zeroSize:
                    updateSqlCdScan(inpath, None, xmlwrite, 'No write: filesize of zero')
                elif str(err).find('CDMS I/O error: End of file') >= 0:
                    updateSqlCdScan(inpath, None, xmlwrite, 'No write: CDMS I/O Error')
                elif str(err).find('RuntimeError: Dimension time in files') >= 0:
                    updateSqlCdScan(inpath, None, xmlwrite, 'No write: File dimension inconsistency')
                elif str(err).find('CDMS I/O error: Determining type of file') >= 0:
                    updateSqlCdScan(inpath, None, xmlwrite, 'No write: CDMS Filetype determination issue')
                elif str(err).find('Cannot allocate memory') >= 0:
                    updateSqlCdScan(inpath, None, xmlwrite, 'No write: Memory allocation problem')  
                elif str(err).find('Invalid relative time units') >= 0:
                    updateSqlCdScan(inpath, None, xmlwrite, 'No write: Invalid relative time units')                      
                else:    
                    updateSqlCdScan(inpath, None, xmlwrite, 'No write')
            else:
                errors = getWarnings(str(err))
                if len(errors) > 0:
                    errorCode = parseWarnings(errors)
                    outfileNew = outfile.replace('0000000',errorCode)
                    os.rename(outfile,outfileNew)
                    outfile = outfileNew
                    if len(errors) > 255:
                        errors = errors[0:255]
                # update database with xml write
                updateSqlCdScan(inpath, outfile, xmlwrite, errors)
        else:
            q = 'select path from paths where xmlFile = \'' + ef[0] + '\';'
            queryResult = sqlQuery(q)
            # if the existing xmlFile is not related to the input path
            # then state there is an existing xml link
            # else populate the current writetime
            xmlwrite = toSQLtime(datetime.datetime.now())
            if len(queryResult) > 0:
                # check if path is the same 
                if len(findInList(inpath, queryResult[0])) == 0:   
                    # update database with xml write
                    updateSqlCdScan(inpath, None, xmlwrite, 'Existing xml link')
            else:
                updateSqlCdScan(inpath, ef[0], xmlwrite, 'Orphan xml')


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)      

#%%
def getWarnings(err):
    errstart = err.find('Warning') ; # Indexing returns value for "W" of warning
    err1 = err.find(' to: [')
    if err1 == -1: err1 = len(err)-1 ; # Warning resetting axis time values
    err2 = err.find(': [')
    if err2 == -1: err2 = len(err)-1 ; # Problem 2 - cdscan error - Warning: axis values for axis time are not monotonic: [
    err3 = err.find(':  [')
    if err3 == -1: err3 = len(err)-1 ; # Problem 2 - cdscan error - Warning: resetting latitude values:  [
    err4 = err.find(') ')
    if err4 == -1: err4 = len(err)-1 ; # Problem 1 - zero infile size ; Problem 4 - no outfile
    err5 = err.find(', value=')
    if err5 == -1: err5 = len(err)-1 ; # Problem 2 - cdscan error - 'Warning, file XXXX, dimension time overlaps file
    errorCode = err[errstart:min(err1,err2,err3,err4,err5)]

    errPython = err.find('Traceback (most recent call last)')
    if errPython > 0:
        errorCode = errorCode + 'Python Error'
    
    return errorCode

def parseWarnings(err):
    errorCode = list('0000000')
    if err.find('dimension time contains values in file') >= 0:
        errorCode[0] = '1'
    if err.find('Warning: Axis values for axis time are not monotonic') >= 0:
        errorCode[1] = '1'
    if err.find('Warning: resetting latitude values') >= 0:
        errorCode[2] = '1'
    if err.find('zero infile size') >= 0:
        errorCode[3] = '1'                        
    if err.find('dimension time overlaps file') >= 0:
        errorCode[4] = '1'
    if err.find('Your first bounds') >= 0:
        errorCode[5] = '1'        
    if err.find('Python Error') >= 0:
        errorCode[6] = '1'                
    errorCode = "".join(errorCode)        
    return errorCode

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

def sqlQuery(query):
    # create database connection
    conn = MySQLdb.connect(host=sqlcfg.mysql_server, user=sqlcfg.mysql_user, password=sqlcfg.mysql_password, database=sqlcfg.mysql_database)
    c = conn.cursor()

    c.execute(query)
    queryResult = c.fetchall()
    queryResult = np.array(queryResult)
    conn.close()

    return queryResult

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

#%% Define functions - moved from make_cmip5_xml1.py
def checkPID(pid):
    """ Check For the existence of a unix pid. """
    # First ensure pid is int type
    pid = int(pid)
    try:
        os.kill(pid,0)
    except OSError:
        return False
    else:
        return True

#%%
def keepFile(outfileName,errStr):
    outfileNameNew = outfileName.replace('.latestX.xml',''.join(['.latestX.WARN',str(errStr),'.xml']))
    if os.path.isfile(outfileName):
        os.rename(outfileName,outfileNameNew)

#%%
def logWrite(logfile,time_since_start,path_name,i1,data_outfiles,len_vars):
    outfile_count = len(data_outfiles)
    time_since_start_s = '%09.2f' % time_since_start
    print("".join([path_name.ljust(13),' scan complete.. ',format(i1,"1d").ljust(6),' paths total; ',str(outfile_count).ljust(6),' output files to be written (',format(len_vars,"1d").ljust(3),' vars sampled)']))
    writeToLog(logfile,"".join([time_since_start_s,' : ',path_name.ljust(13),' scan complete.. ',format(i1,"1d").ljust(6),' paths total; ',format(outfile_count,"1d").ljust(6),' output files to be written (',format(len_vars,"1d").ljust(3),' vars sampled)']))
    return

#%%
def pathToFile(inpath,start_time,queue1):
#$#def pathToFile(inpath,start_time): #; # Non-parallel version of code for testing
    data_paths = [] ; i1 = 0
    #for (path,dirs,files) in os.walk(inpath,topdown=True):
    for (path,dirs,files) in scandir.walk(inpath,topdown=True):

        ## IGNORE EXPERIMENTS - AT SEARCH LEVEL - SPEED UP SEARCH ##
        expExclude = set(['aqua4K','aqua4xCO2','aquaControl','esmControl','esmFdbk1','esmFixClim1',
                          'esmFixClim2','esmHistorical','esmrcp85','Igm','midHolocene','sst2030','sst2090','sst2090rcp45',
                          'sstClimAerosol','sstClimSulfate','volcIn2010'])
        timeExclude = set(['3hr','6hr','day','monClim','yr'])
        reDec = re.compile(r'decadal[0-9]{4}')
        reVol = re.compile(r'noVolc[0-9]{4}')
        dirs[:] = [d for d in dirs if d not in expExclude]
        dirs[:] = [d for d in dirs if d not in timeExclude and not re.search(reDec,d) and not re.search(reVol,d)]
        ## IGNORE EXPERIMENTS ##

        # Test files don't exist and we're not at the end of the directory tree
        if files == [] and dirs != []:
            continue ; #print 'files&dirs',path

        ## BAD DIRECTORIES - TRUNCATE INVALID MD5 DATA ##
        if re.search(r'bad[0-9]',path):
            continue ; #print 're.search',path
        if '-will-delete' in path:
            print('-will-delete',path)
            continue
        # Iterate and purge bad[0-9] subdirs
        for dirCount,el in reversed(list(enumerate(dirs))):
            if re.match(r'bad[0-9]',el):
                del dirs[dirCount]

        #130225 1342: Pathologies to consider checking for bad data
        #badpaths = ['/bad','-old/','/output/','/ICHEC-old1/']
        #bad = GISS-E2-R, EC-EARTH ; -old = CSIRO-QCCCE-old ; /output/ = CSIRO-Mk3-6-0 ; /ICHEC-old1/ = EC-EARTH
        #paths rather than files = CNRM-CM5, FGOALS-g2, bcc-csm1-1
        #duplicates exist between /cmip5_gdo2/scratch and /cmip5_css02/scratch = CCSM4, CSIRO-Mk3-6-0
        ## BAD DIRECTORIES ##

        if files != [] and dirs == []:
            # Append to list variable
            #$#print i1,path #$#
            data_paths += [path]
            i1 = i1 + 1 ; # Increment counter

    # Create variable and realm names
    experiments     = ['1pctCO2','abrupt4xCO2','amip','amip4K','amip4xCO2','amipFuture','historical','historicalExt',
                       'historicalGHG','historicalMisc','historicalNat','past1000','piControl','rcp26','rcp45','rcp60','rcp85',
                       'sstClim','sstClim4xCO2'] ; experiments.sort()
    temporal        = ['fx','mon'] ; # For months and fixed fields only
    atmOrocn        = ['atm','ocn'] ; atmOrocn.sort()
    atm_vars        = ['cl','clcalipso','cli','clisccp','clivi','clt','clw','clwvi','evspsbl','hfls','hfss','hur','hurs',
                       'hus','huss','mc','pr','prc','prsn','prw','ps','psl','rlds','rldscs','rlus','rluscs','rlut',
                       'rlutcs','rsds','rsdscs','rsdt','rsus','rsuscs','rsut','rsutcs','sbl','sci','sfcWind',
                       'ta','tas','tasmax','tasmin','tauu','tauv','ts','ua','uas','va','vas','wap','zg'] ; atm_vars.sort()
    fx_vars         = ['areacella','areacello','basin','deptho','mrsofc','orog','sftgif','sftlf','sftof','volcello'] ; fx_vars.sort()
    land_vars       = ['cropfrac','evspsblsoi','evspsblveg','gpp','lai','mrfso','mrro','mrros','mrso','mrsos','tran','tsl'] ; land_vars.sort()
    ocn_vars        = ['agessc','cfc11','dissic','evs','ficeberg','friver','hfds','hfls','hfss','mfo','mlotst','omlmax','ph','pr','rlds',
                       'rhopoto','rsds','sfriver','so','soga','sos','tauuo','tauvo','thetao','thetaoga','tos','uo','vo','vsf','vsfcorr',
                       'vsfevap','vsfpr','vsfriver','wfo','wfonocorr','zos','zostoga'] ; ocn_vars.sort()
    seaIce_vars     = ['sic','sim','sit','snc','snd'] ; seaIce_vars.sort()
    landIce_vars    = ['snc','snd','snw','tpf','pflw'] ; landIce_vars.sort()
    list_vars       = atm_vars+fx_vars+land_vars+ocn_vars+seaIce_vars+landIce_vars ; # Create length counter for reporting
    len_vars        = len(list_vars) ; # Create length counter for reporting

    # Check for valid outputs
    if not data_paths:
        #print "** No valid data found on path.. **"
        # Create timestamp as function completes
        time_since_start = time.time() - start_time
        #$#return('','',time_since_start,i1,0,len_vars) ; # Non-parallel version of code for testing
        queue1.put_nowait(['','',time_since_start,i1,0,len_vars]) ; # Queue
        return

    # Mine inputs for info and create outfiles and paths
    data_outfiles,data_outfiles_paths = [[] for _ in range(2)] ; i2 = 0
    for path in data_paths:
        path_bits   = path.split('/')
        # Set indexing - first data/scratch
        if 'data' in path_bits:
            pathIndex = path_bits.index('data')
        elif 'scratch' in path_bits:
            pathIndex = path_bits.index('scratch')
        # Next find DRS start index
        # Naming obtained from http://cmip-pcmdi.llnl.gov/cmip5/docs/cmip5_data_reference_syntax.pdf
        ActivityTest    = re.compile('cmip[5-6]$')
        ProductTest     = re.compile('^output') ; # Most conform to output[1-3], however CSIRO-Mk3-6-0 doesn't
        CMIPIndex       = [ i for i, item in enumerate(path_bits) if re.match(ActivityTest,item) ][-1] ; # Get last entry
        if re.search(ProductTest,path_bits[CMIPIndex+1]):
            DRSStartIndex = CMIPIndex+2
        else:
            print(path_bits)
        # Use indices to build output filenames
        try:
            #institute   = path_bits[DRSStartIndex]
            model       = path_bits[DRSStartIndex+1] ; #4,6
            experiment  = path_bits[DRSStartIndex+2] ; #5,7
            time_ax     = path_bits[DRSStartIndex+3] ; #6,8
            realm       = path_bits[DRSStartIndex+4] ; #7,9
            tableId     = path_bits[DRSStartIndex+5] ; #8,10
            # Fix realms to standard acronyms
            if (realm == 'ocean'):
                realm = 'ocn'
            elif (realm == 'atmos'):
                realm = 'atm'
            realisation = path_bits[DRSStartIndex+6] ; #9,11
            # Check for source path and order variable/version info
            if path_bits[DRSStartIndex+7] in list_vars:
                variable    = path_bits[DRSStartIndex+7] ; #10
                version     = path_bits[DRSStartIndex+8] ; #11
            elif path_bits[DRSStartIndex+8] in list_vars:
                version     = path_bits[DRSStartIndex+7] ; #10
                variable    = path_bits[DRSStartIndex+8] ; #11
                #if 'data' in path_bits:
                #    print path
            else:
                # Cases where variables are not in list_vars
                #print model,experiment,time_ax,realm,tableId,'10:',path_bits[pathIndex+10],'11:',path_bits[pathIndex+11]
                continue
            # Getting versioning/latest info
            testfile = os.listdir(path)[0]
            # Test for zero-size file before trying to open
            fileinfo = os.stat(os.path.join(path,testfile))
            checksize = fileinfo.st_size
            if checksize == 0:
                continue ; #print "".join(['Zero-sized file: ',path])
            # Read access check
            if os.access(os.path.join(path,testfile),os.R_OK) != True:
                continue ; #print "".join(['No read permissions: ',path])
            # Netcdf metadata scour
            #f_h = cdm.open(os.path.join(path,testfile))
            tracking_id     = '' ; #tracking_id     = f_h.tracking_id
            creation_date   = '' ; #creation_date   = f_h.creation_date
            #f_h.close()
            if testLatest(tracking_id,creation_date):
                lateststr = 'latestX' ; #lateststr = 'latest1' ; # Latest
            else:
                lateststr = 'latest0' ; # Not latest
        except Exception as err:
            # Case HadGEM2-AO - attempt to recover data
            if 'HadGEM2-AO' in model and experiment in ['historical','rcp26','rcp45','rcp60','rcp85']:
                variable    = path_bits[pathIndex+8]
                if realm in   'atm':
                    tableId = 'Amon'
                elif realm in 'ocn':
                    tableId = 'Omon'
                elif realm in 'landIce':
                    tableId = 'LImon'
                elif realm in 'land':
                    tableId = 'Lmon'
                elif realm in 'seaIce':
                    tableId = 'OImon'
                version     = datetime.datetime.fromtimestamp(fileinfo.st_ctime).strftime('%Y%m%d')
            # Case BESM-OA2-3 - skip as only decadal data
            elif 'BESM-OA2-3' in model and 'decadal' in experiment:
                continue
            else:
                print('pathToFile - Exception:',err,path)
                continue
        # Test for list entry and trim experiments and variables to manageable list
        if (experiment in experiments) and (time_ax in temporal) and (variable in list_vars):
            data_outfiles.insert(i2,".".join(['cmip5',model,experiment,realisation,time_ax,realm,tableId,variable,"".join(['ver-',version]),lateststr,'xml']))
            data_outfiles_paths.insert(i2,path)
            i2 = i2 + 1

    # Create timestamp as function completes
    time_since_start = time.time() - start_time

    #$#return(data_outfiles,data_outfiles_paths,time_since_start,i1,i2,len_vars) ; # Non-parallel version of code for testing
    queue1.put_nowait([data_outfiles,data_outfiles_paths,time_since_start,i1,i2,len_vars]) ; # Queue
    return

#%%
def testLatest(tracking_id,creation_date):
    # There is a need to map models (rather than institutes) to index nodes as NSF-DOE-NCAR has multiple index nodes according to Karl T
    # User cmip5_controlled_vocab.txt file: http://esg-pcmdi.llnl.gov/internal/esg-data-node-documentation/cmip5_controlled_vocab.txt
    # This maps institute_id => (data_node, index_node)
    # where data_node is the originator of the data, and index_node is where they publish to.
    instituteDnodeMap = {
        'BCC':('bcccsm.cma.gov.cn', 'pcmdi9.llnl.gov'),
        'BNU':('esg.bnu.edu.cn', 'pcmdi9.llnl.gov'),
        'CCCMA':('dapp2p.cccma.ec.gc.ca', 'pcmdi9.llnl.gov'),
        'CCCma':('dapp2p.cccma.ec.gc.ca', 'pcmdi9.llnl.gov'),
        'CMCC':('adm07.cmcc.it', 'adm07.cmcc.it'),
        'CNRM-CERFACS':('esg.cnrm-game-meteo.fr', 'esgf-node.ipsl.fr'),
        'COLA-CFS':('esgdata1.nccs.nasa.gov', 'esgf.nccs.nasa.gov'),
        'CSIRO-BOM':('esgnode2.nci.org.au', 'esg2.nci.org.au'),
        'CSIRO-QCCCE':('esgnode2.nci.org.au', 'esg2.nci.org.au'),
        'FIO':('cmip5.fio.org.cn', 'pcmdi9.llnl.gov'),
        'ICHEC':('esg2.e-inis.ie', 'esgf-index1.ceda.ac.uk'),
        'INM':('pcmdi9.llnl.gov', 'pcmdi9.llnl.gov'),
        'IPSL':('vesg.ipsl.fr', 'esgf-node.ipsl.fr'),
        'LASG-CESS':('esg.lasg.ac.cn', 'pcmdi9.llnl.gov'),
        'LASG-IAP':('esg.lasg.ac.cn', 'pcmdi9.llnl.gov'),
        'LASF-CESS':('esg.lasg.ac.cn', 'pcmdi9.llnl.gov'),
        'MIROC':('dias-esg-nd.tkl.iis.u-tokyo.ac.jp', 'pcmdi9.llnl.gov'),
        'MOHC':('cmip-dn1.badc.rl.ac.uk', 'esgf-index1.ceda.ac.uk'),
        'MPI-M':('bmbf-ipcc-ar5.dkrz.de', 'esgf-data.dkrz.de'),
        'MRI':('dias-esg-nd.tkl.iis.u-tokyo.ac.jp', 'pcmdi9.llnl.gov'),
        'NASA GISS':('esgdata1.nccs.nasa.gov', 'esgf.nccs.nasa.gov'),
        'NASA-GISS':('esgdata1.nccs.nasa.gov', 'esgf.nccs.nasa.gov'),
        'NASA GMAO':('esgdata1.nccs.nasa.gov', 'esgf.nccs.nasa.gov'),
        'NCAR':('tds.ucar.edu', 'esg-datanode.jpl.nasa.gov'),
        'NCC':('norstore-trd-bio1.hpc.ntnu.no', 'pcmdi9.llnl.gov'),
        'NICAM':('dias-esg-nd.tkl.iis.u-tokyo.ac.jp', 'pcmdi9.llnl.gov'),
        'NIMR-KMA':('pcmdi9.llnl.gov', 'pcmdi9.llnl.gov'),
        'NOAA GFDL':('esgdata.gfdl.noaa.gov', 'pcmdi9.llnl.gov'),
        'NOAA-GFDL':('esgdata.gfdl.noaa.gov', 'pcmdi9.llnl.gov'),
        'NSF-DOE-NCAR':('tds.ucar.edu', 'esg-datanode.jpl.nasa.gov'),
    }
    masterDnodes = {
        'adm07.cmcc.it':'',
        'esg-datanode.jpl.nasa.gov':'',
        'esg2.nci.org.au':'',
        'esgf-data.dkrz.de':'',
        'esgf-index1.ceda.ac.uk':'',
        'esgf-node.ipsl.fr':'',
        'esgf.nccs.nasa.gov':'',
        'pcmdi9.llnl.gov':'',
    }
    modelInstituteMap = {
        'access1-0':'CSIRO-BOM',
        'access1-3':'CSIRO-BOM',
        'bcc-csm1-1':'BCC',
        'noresm-l':'NCC',
    }
    #cmd = ''.join(['/work/durack1/Shared/cmip5/esgquery_index.py --type f -t tracking_id:',tracking_id,' -q latest=true --fields latest'])
    # try esgquery_index --type f -t tracking_id='tracking_id',latest=true,index_node='index_node' ; # Uncertain if index_node is available
    #tmp = os.popen(cmd).readlines()
    #time.sleep(1) ; # Pause
    #latestbool = False
    #for t in tmp:
    #    if find(t,'latest'):
    #        latestbool = True
    latestbool = True

    return latestbool

#%%
def xmlLog(logFile,fileZero,fileWarning,fileNoWrite,fileNoRead,fileNone,errorCode,batchPrint,inpath,outfileName,time_since_start,i,xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5,xmlGood):
    time_since_start_s = '%09.2f' % time_since_start
    logtime_now = datetime.datetime.now()
    logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
    if fileZero:
        # Case cdscan writes no file
        if '/data/cmip5/' in inpath:
            err_text = ' DATA PROBLEM 1 (cdscan error - zero infile size) indexing '
        else:
            err_text = ' PROBLEM 1 (cdscan error - zero infile size) indexing '
        writeToLog(logFile,"".join(['** ',format(xmlBad1,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,inpath,' **']))
        if batchPrint:
            print("".join(['**',err_text,inpath,' **']))
        xmlBad1 = xmlBad1 + 1;
        # Rename problem files
        keepFile(outfileName,1)
    elif fileWarning:
        # Case cdscan reports an error
        if '/data/cmip5/' in inpath:
            err_text = "".join([' DATA PROBLEM 2 (cdscan error- \'',errorCode,'\') indexing '])
        else:
            err_text = "".join([' PROBLEM 2 (cdscan error - \'',errorCode,'\') indexing '])
        writeToLog(logFile,"".join(['** ',format(xmlBad2,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,inpath,' **']))
        if batchPrint:
            print("".join(['**',err_text,inpath,' **']))
        xmlBad2 = xmlBad2 + 1;
        # Rename problem files
        keepFile(outfileName,2)
    elif fileNoRead:
        # Case cdscan reports no error, however file wasn't readable
        if '/data/cmip5/' in inpath:
            err_text = ' DATA PROBLEM 3 (read perms) indexing '
        else:
            err_text = ' PROBLEM 3 (read perms) indexing '
        writeToLog(logFile,"".join(['** ',format(xmlBad3,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,inpath,' **']))
        if batchPrint:
            print("".join(['**',err_text,inpath,' **']))
        xmlBad3 = xmlBad3 + 1;
        # Rename problem files
        keepFile(outfileName,3)
    elif fileNoWrite:
        # Case cdscan reports no error, however file wasn't written
        if '/data/cmip5/' in inpath:
            err_text = ' DATA PROBLEM 4 (no outfile) indexing '
        else:
            err_text = ' PROBLEM 4 (no outfile) indexing '
        writeToLog(logFile,"".join(['** ',format(xmlBad4,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,inpath,' **']))
        if batchPrint:
            print("".join(['**',err_text,inpath,' **']))
        xmlBad4 = xmlBad4 + 1;
        # Rename problem files
        keepFile(outfileName,4)
    elif fileNone:
        # Case cdscan reports no error, however file wasn't written
        if '/data/cmip5/' in inpath:
            err_text = ' DATA PROBLEM 5 (no infiles) indexing '
        else:
            err_text = ' PROBLEM 5 (no infiles) indexing '
        writeToLog(logFile,"".join(['** ',format(xmlBad5,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,inpath,' **']))
        if batchPrint:
            print("".join(['**',err_text,inpath,' **']))
        xmlBad5 = xmlBad5 + 1;
        # Rename problem files
        keepFile(outfileName,5)
    else:
        writeToLog(logFile,"".join(['** ',format(xmlGood,"07d"),' ',logtime_format,' ',time_since_start_s,'s success creating: ',outfileName,' **']))
        xmlGood = xmlGood + 1;

    return[xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5,xmlGood] # ; Non-parallel version of code
    #queue1.put_nowait([xmlBad1,xmlBad2,xmlBad3,xmlBad4,xmlBad5,xmlGood]) ; # Queue
    #return

#%%
def xmlWrite(inpath,outfile,host_path,cdat_path,start_time,queue1):
    infilenames = glob(os.path.join(inpath,'*.nc'))
    # Create list of fx vars
    fx_vars = ['areacella','areacello','basin','deptho','mrsofc','orog','sftgif','sftlf','sftof','volcello'] ; fx_vars.sort()
    # Construct outfile path from outfilename
    outfile_string  = "".join(outfile)
    outfile_bits    = outfile_string.split('.')
    experiment      = outfile_bits[2]
    temporal        = outfile_bits[4]
    if (temporal == 'mon'):
        temporal = 'mo_new' ; # Updated path; existing xmls are in place until successful xml write completion
    elif (temporal == 'fx'):
        temporal = 'fx_new' ;

    realm           = outfile_bits[5]
    variable        = outfile_bits[7]
    if (variable in fx_vars):
        realm = 'fx'
        # Truncate experiment from fx files
        out_path = os.path.join(realm,temporal,variable)
    else:
        out_path = os.path.join(experiment,realm,temporal,variable)

    outfileName = os.path.join(host_path,out_path,"".join(outfile))
    outfileName = outfileName.replace('.mon.','.mo.')
    if not os.path.exists(os.path.join(host_path,out_path)):
        # At first run create output directories
        try:
            #os.makedirs(os.path.join(host_path,out_path))
            mkDirNoOSErr(os.path.join(host_path,out_path)) ; # Alternative call - don't crash if directory exists
        except Exception as err:
            print('xmlWrite - Exception:',err)
            print("".join(['** Crash while trying to create a new directory: ',os.path.join(host_path,out_path)]))

    if os.path.isfile(outfileName):
        os.remove(outfileName)

    # Generate xml file - and preallocate error codes
    fileWarning = False
    errorCode   = ''
    fileNoWrite = False
    fileNoRead  = False
    fileZero    = False
    fileNone    = False
    if len(infilenames) != 0:
        # Create a fullpath list of bad files and exclude these, by trimming them out of a filename list
        cmd = "".join([cdat_path,'cdscan -x ',outfileName,' ',os.path.join(inpath,'*.nc')])
        # Catch errors with file generation
        p = Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE)
        out,err = p.communicate()
        # Check cdscan output for warning flags
        fileWarning = False
        if err.find('Warning') > -1:
            errstart = err.find('Warning') ; # Indexing returns value for "W" of warning
            err1 = err.find(' to: [')
            if err1 == -1: err1 = len(err)-1 ; # Warning resetting axis time values
            err2 = err.find(': [')
            if err2 == -1: err2 = len(err)-1 ; # Problem 2 - cdscan error - Warning: axis values for axis time are not monotonic: [
            err3 = err.find(':  [')
            if err3 == -1: err3 = len(err)-1 ; # Problem 2 - cdscan error - Warning: resetting latitude values:  [
            err4 = err.find(') ')
            if err4 == -1: err4 = len(err)-1 ; # Problem 1 - zero infile size ; Problem 4 - no outfile
            err5 = err.find(', value=')
            if err5 == -1: err5 = len(err)-1 ; # Problem 2 - cdscan error - 'Warning, file XXXX, dimension time overlaps file
            errorCode = err[errstart:min(err1,err2,err3,err4,err5)]
            fileWarning = True
        elif err.find(''.join(['Variable \'',variable,'\' is duplicated'])) > -1:
            err6Str = ''.join(['Variable \'',variable,'\' is duplicated'])
            errstart = err.find(err6Str)
            errorCode = err[errstart:len(err)-1] ; # Problem 4 - cdscan error - 'Variable \'%s\' is duplicated - RunTimeError
            #fileWarning = True ; # Not set to true as no file is generated - so caught by fileNoWrite below
        else:
            errorCode = ''
        #if err.find
        del(cmd,err,out,p) ; gc.collect()
        # Validate outfile was written
        fileNoWrite = not os.path.isfile(outfileName)
        # Validate infile is readable (permission error) and non-0-file size
        fileNoRead = False
        fileZero = False
        filestocheck = os.listdir(inpath)
        for checkfile in filestocheck:
            # 0-file size check
            fileinfo = os.stat(os.path.join(inpath,checkfile))
            checksize = fileinfo.st_size
            if checksize == 0:
                fileZero = True
            # Read access check
            if os.access(os.path.join(inpath,checkfile),os.R_OK) != True:
                fileNoRead = True
    else:
        fileNone = True

    # Create timestamp as function completes
    time_since_start = time.time() - start_time

    #return(inpath,outfileName,fileZero,fileWarning,fileNoRead,fileNoWrite,fileNone,errorCode,time_since_start) ; Non-parallel version of code
    queue1.put_nowait([inpath,outfileName,fileZero,fileWarning,fileNoRead,fileNoWrite,fileNone,errorCode,time_since_start]) ; # Queue
    return

#%%
def writeToLog(logFilePath,textToWrite):
    """
    Documentation for writeToLog(logFilePath,textToWrite):
    -------
    The writeToLog() function writes specified text to a text log file

    Author: Paul J. Durack : pauldurack@llnl.gov

    Usage:
    ------
        >>> from durolib import writeToLog
        >>> writeToLog(~/somefile.txt,'text to write to log file')

    Notes:
    -----
        Current version appends a new line after each call to the function.
        File will be created if it doesn't already exist, otherwise new text
        will be appended to an existing log file.
    """
    if os.path.isfile(logFilePath):
        logHandle = open(logFilePath,'a') ; # Open to append
    else:
        logHandle = open(logFilePath,'w') ; # Open to write
    logHandle.write("".join([textToWrite,'\n']))
    logHandle.close()

#%%
def mkDirNoOSErr(newdir,mode=0o777):
    """
    Documentation for mkDirNoOSErr(newdir,mode=0777):
    -------
    The mkDirNoOSErr() function mimics os.makedirs however does not fail if the directory already
    exists

    Author: Paul J. Durack : pauldurack@llnl.gov

    Returns:
    -------
           Nothing.
    Usage:
    ------
        >>> from durolib import mkDirNoOSErr
        >>> mkDirNoOSErr('newPath',mode=0777)

    Notes:
    -----
    """
    try:
        os.makedirs(newdir,mode)
    except OSError as err:
        #Re-raise the error unless it's about an already existing directory
        if err.errno != errno.EEXIST or not os.path.isdir(newdir):
            raise


