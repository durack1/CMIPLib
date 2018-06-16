#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Documentation for make_cmip5_xml2:
-----
Created on Wed Apr 11 10:59:24 2018

Paul J. Durack and Stephen Po-Chedley 11th April 2018

Script is meant to create xml library in several parts:
    + Find all CMIP5 directory paths 
    + Write paths and filesystem datestamps to SQL database
    + Create xmls
    + Running the script subsequently will only update xml files
      that correspond to directories in which the files have been 
      modified (using the SQL DB as a reference)

|  SP  11 Apr 2018  -   Initial outline/functionality of xml processing
|  SP  16 Jun 2018  -   Updated library and database, speed improvements, added  
                        functionality to retire paths

@author: pochedls
"""

import sys
sys.path.append('lib/')
import CMIPLib
import time 

print('Started on: ', time.ctime()) # start time for reference
t00 = time.time() # time whole script

# Define search directories
data_directories = ['/p/css03/cmip5_css01/data/cmip5/output1/', '/p/css03/cmip5_css01/data/cmip5/output2/',
                    '/p/css03/cmip5_css02/data/cmip5/output1', '/p/css03/cmip5_css02/data/cmip5/output2', 
                    '/p/css03/scratch/cmip5/', '/p/css03/scratch/published-latest/cmip5/',
                    '/p/css03/scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/',
                    '/p/css03/scratch/published-older/cmip5/', '/p/css03/scratch/should-publish/cmip5/',
                    '/p/css03/scratch/unknown-dset/cmip5/', '/p/css03/scratch/unknown-status/cmip5/',
                    '/p/css03/scratch/unknown-status/cmip5/', '/p/css03/scratch/obsolete/cmip5/',
                    '/p/css03/esgf_publish/cmip5/']

# update SQL DB - parallelize here
for parent in data_directories:
    t0 = time.time() # start timer
    try:
        CMIPLib.updateSqlDb(parent) # this line updates the db
    except:
        print('Error scanning ' + parent)
    t1 = time.time() # end timer
    print(t1-t0) # print timing
    print()

# get directories to scan
dirs_to_scan = CMIPLib.getScanList(variables=['tas'])

# example updating database with xml write
p = '/p/css03/scratch/cmip5/output1/CNRM-CERFACS/CNRM-CM5/historicalNat/mon/atmos/Amon/r1i1p1/v20110902/prc/'
xmlFile = 'test.xml'
xmlwrite = '2018-01-01 00:00:00'
error = 'FALSE'
CMIPLib.updateSqlCdScan(p, xmlFile, xmlwrite, error)

t1 = time.time()
total = t1-t00
print(total)




