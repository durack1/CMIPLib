#! /usr/bin/env python

# written by Jason Boutte
# a few comments and minor changes added by Jeff Painter

#
# Commands:
#
# Search: This will downloads a json clone of all the published esgf data.
#  Requires a url, e.g. http://pcmdi.llnl.gov/esg-search/search
#
# Filter: This will let you filter by a data_node, creates two files datasets_%data_node%.json and
# datasets_other.json. Each file is a dictionary where the keys are dataset master ids and the
# values are dictionaries with version as the key and a list of data_nodes who have those versions.
#
# Analysis: This will process the output files from Filter. Creates 5 csv files;
#  latest   will have the which datasets are the latest,
#  missing  will have datasets that are missing,
#  multiple will have datasets where the data_node has multiple versions,
#  multiple_latest
#           will have the datasets that have multiple versions where the latest version is included,
#  replace  will have the datasets that we have but are not the latest.
#
# To see logging output, add the command-line argument "--log".

import argparse
import json
import logging, pdb
import re
import os
from pprint import pprint

import pandas as pd  # Jason recommends using pip or conda to get pandas and requests
import requests
from find_publishable import vnum # vnum converts 'v20120321' to the number 20120321
# Why vnum? 'v4'>'v20131108' but 4<20131108.  'v4'>'v2' and 4>2.  Thus the numeric comparison
# is the only choice to reliably identifies the latest version, 20131108 and 4 in these cases.

logger = logging.getLogger(__name__)

def process_out_dir(path):
    # Ensures that the supplied path exists and returns it.
    # If the supplied path is None, returns the path from which this script was loaded.
    if path is None:
        path = os.path.dirname(os.path.realpath(__file__))
        print "output path is", path

    if not os.path.exists(path):
        os.mkdir(path)

    return path

def write_csv(filename, data):
    # Writes the data into a file 'filename', in csv (comma separated values) format.
    with open(filename, 'w') as out_file:
        for d in data:
            out_file.write('%s\n' % (','.join(d)))

def esg_analysis(args):
    input = args.input
    output = process_out_dir(args.output)
    if input is None:
        input = os.path.dirname(os.path.realpath(__file__))

    filtered = None
    other = None

    for root, _, files in os.walk(input):
        for f in files:
            if f == 'datasets_other.json':
                other = os.path.join(root, f)
            elif re.match('datasets_.*\.json', f) is not None:
                filtered = os.path.join(root, f)

    result = re.match('^.*/datasets_(.*)\.json$', filtered)

    data_node = result.group(1)

    with open(filtered, 'r') as in_file:
        dn = json.load(in_file)

    with open(other, 'r') as in_file:
        on = json.load(in_file)

    logger.info('Loaded %s datasets for %s data node, file %s', len(dn), data_node, filtered)

    logger.info('Loaded %s datasets for other data nodes, file %s', len(on), other)

    latest = []
    replace = []

    for mid, data in dn.iteritems():
        latest_ver = sorted(data.keys())[-1]

        if mid not in on:
            latest.append([mid, latest_ver])
        else:
            candidates = sorted(on[mid].keys(),cmp=(lambda x,y: vnum(x)-vnum(y)), reverse=True)

            really_latest = vnum(latest_ver)
            for candidate in candidates:
                if really_latest >= vnum(candidate):
                    pass
                else:  # candidate is a later version number than we've found
                    really_latest = vnum(candidate)
                    item = [mid, latest_ver, candidate]
                    item.extend(on[mid][candidate]) # [dataset, old vers, new vers, new url 1, new url 2, ...]
                    replace.append(item)
            if really_latest==vnum(latest_ver):
                latest.append([mid, latest_ver]) # latest item = [dataset, version]


    write_csv('%s_latest.csv' % (data_node,), latest)
    logger.info('%s has %s datasets that are the latest version', data_node, len(latest))

    write_csv('%s_replace.csv' % (data_node,), replace)
    logger.info('%s has %s datasets that are not the latest version', data_node, len(replace))

    missing = []

    for mid, data in on.iteritems():
        if mid not in dn:
            latest = sorted(data.keys())[-1]
            item = [mid, latest]
            item.extend(data[latest])
            missing.append(item)

    write_csv('%s_missing.csv' % (data_node,), missing)
    logger.info('%s is missing %s datasets', data_node, len(missing))

    multiple = []
    multiple_latest = []

    for mid, data in dn.iteritems():
        if len(data) > 1:
            latest = sorted(data.keys())[-1]

            item = [mid, latest]

            multiple.append(item)

            if mid not in on:
                multiple_latest.append(item)
            else:
                candidate = sorted(on[mid].keys())

                if latest >= candidate:
                    multiple_latest.append(item)

    write_csv('%s_mutiple.csv' % (data_node,), multiple)

    logger.info('%s has %s datasets that have multiple versions', data_node, len(multiple))

    write_csv('%s_multiple_latest.csv' % (data_node,), multiple_latest)

    logger.info('%s has %s datasets that have multiple version of which the latest is contained', data_node, len(multiple_latest))

def esg_populate_versions(group, filename):
    results = {}

    for k, g in group:
        versions = {}

        for i, r in g.iterrows():
            ver = r['version']

            if ver in versions:
                versions[ver].append(r['data_node'])
            else:
                versions[ver] = [r['data_node']]

        results[k] = versions

    with open(filename, 'w') as out_file:
        json.dump(results, out_file)

def esg_filter(args):
    data_node = args.data_node
    in_file = os.path.realpath(args.input)
    out_dir = process_out_dir(args.output)

    df = pd.read_json(in_file)

    logger.info('Loaded %s entries' % (len(df),))

    version_col = df.apply(lambda x: x['instance_id'].split('.')[-1], 1)

    df['version'] = version_col

    target_dn = df[df['data_node'] == data_node]

    target_grouped = target_dn.groupby('master_id')

    logger.info('Datanode %s has %s datasets, %s which are unique' % (data_node, len(target_dn), len(target_grouped)))

    esg_populate_versions(target_grouped, 'datasets_%s.json' % (data_node,))

    other_dn = df[df['data_node'] != data_node]

    other_grouped = other_dn.groupby('master_id')

    logger.info('Other datanodes have %s datasets, %s which are unique' % (len(other_dn), len(other_grouped)))

    esg_populate_versions(other_grouped, 'datasets_other.json')

def esg_search(args):
    params = {
            'facets': '*',
            'distrib': 'true',
            'offset': 0,
            'limit': 10000,
            'project': args.project,
            'format': 'application/solr+json',
            }
# example of params to narrow this down:
#            'institute': 'ICHEC',
#            'experiment': 'rcp85',
#            'time_frequency': '6hr',
#            'realm': 'atmos',
#            'ensemble': 'r8i1p1',

    offset = 0
    count = 0
    total = None
    output = process_out_dir(args.output)

    output = os.path.join(output, 'esg-data.json')

    docs = []

    while True:
        params['offset'] = offset

        response = requests.get(args.url, params=params)

        data = json.loads(response.text)

        if total is None:
            total = data['response']['numFound']

        num = len(data['response']['docs'])

        logger.info('Retrieved slice at offset %s with %s items' % (offset, num))

        docs += data['response']['docs']

        offset += num

        if offset >= total:
            break

    logger.info('Writing output to %s', output)

    with open(output, 'w') as outfile:
        json.dump(docs, outfile)

def create_parser():
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('--log', action='store_true')

    parser = argparse.ArgumentParser(add_help=False)
    subparsers = parser.add_subparsers()

    search = subparsers.add_parser('search', parents=[parent_parser])
    search.add_argument('url', type=str)
    search.add_argument('-p', '--project', nargs='?', default='CMIP5', type=str)
    search.add_argument('-o', '--output', nargs='?', type=str)
    search.add_argument('-f', '--facets', nargs='?', type=str) #jfp
    search.set_defaults(func=esg_search)

    filter = subparsers.add_parser('filter', parents=[parent_parser])
    filter.add_argument('input', type=str)
    filter.add_argument('-d', '--data_node', nargs='?', default='aims3.llnl.gov', type=str)
    filter.add_argument('-o', '--output', nargs='?', type=str)
    filter.set_defaults(func=esg_filter)

    analysis = subparsers.add_parser('analysis', parents=[parent_parser])
    analysis.add_argument('-i', '--input', nargs='?', type=str)
    analysis.add_argument('-o', '--output', nargs='?', type=str)
    analysis.set_defaults(func=esg_analysis)

    return parser

parser = create_parser()

args = parser.parse_args()

if args.log:
    logging.basicConfig(level=logging.DEBUG)

args.func(args)
