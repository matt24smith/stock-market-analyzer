# -*- coding: utf-8 -*-
"""

Author:     Matt Smith
Date:       2017-12-07

###############################################################################
#                                                                             #
#                              Important README:                              #
#                                                                             #
# Practice data is sourced from the following github repo. To load it in,     #
# simply download the .zip of the repo, unzip it, and point the "datapath"    #
# variable to the unzipped folder location.                                   #
#                                                                             #
# Repo link:                                                                  #
# https://github.com/eliangcs/pystock-data                                    #
#                                                                             #
###############################################################################

Some stats on the dataset:
    + 9.2m datapoints
    + Data collected from 2015-03-24 to 2017-03-31 (3 years of data)
    + Represents US Stock data

"""

datapath = r"D:\matt-workspace\python_sandbox\stocks\data\pystock-data-gh-pages"


import os
import gzip
import numpy as np
import psutil
import hdbscan
import datetime

epochT2 = datetime.datetime.fromtimestamp(0)

def memory(): return psutil.virtual_memory().percent


def epoch3(t):
    """
    Faster version to convert from string to epoch int '%Y-%m-%d %H:%M:%S UTC'
    031157ZJUN15
    """
    delta = datetime.datetime.strptime(t, '%Y-%m-%d') - epochT2
    t = delta.total_seconds()
    return int(t)


def cluster(revenues_assets, netincome_assets, minClustSize, minSamples):
    """
    Returns a clustering on revenue/time and netincome/time data
    """
    x = zip(revenues_assets, netincome_assets)
    clusterer = (hdbscan.HDBSCAN(algorithm='prims_kdtree',
                 min_cluster_size=minClustSize,
                 min_samples=minSamples,
                 prediction_data=True,
                 cluster_selection_method='leaf',
                 gen_min_span_tree=False,
                 metric='euclidean').fit(x))
    return clusterer


datafiles = [os.path.join(root, name)
             for root, dirs, files in os.walk(datapath)
             for name in files
             if name.endswith(".tar.gz")]

count = 0

symbolsdict = {}
stocks = {}

# feed me moar data
for data in datafiles[3:]:
    if memory() > 75:
        print "\nRunning out of memory! Data parsing has been halted."
        break

    print "processing file %s" % data
    with gzip.open(data, 'rb') as f:
        for line in f:
            if '\x00' in line: continue
            if ".txt" in line or "\t" in line:
                symbol = True
                price  = False
                report = False
            elif "," in line and not ",Q" in line and not ",F" in line:
                symbol = False
                price  = True
                report = False
            elif ",Q" in line or ",F" in line:
                symbol = False
                price  = False
                report = True

            if symbol:
                splitline = line.split("\t")
                symbolsdict[splitline[0]] = splitline[1].rstrip()

            if price:
                splitline = line.split(",")
                symbol = splitline[0]
                try:
                    stocks[symbol]
                except KeyError:
                    stocks[symbol] = {}
                try:
                    stocks[symbol]['prices']
                except KeyError:
                    stocks[symbol]['prices'] = np.array([])
                pricedatum = {}
                pricedatum['date'] = splitline[1]
                pricedatum['open'] = splitline[2]
                pricedatum['high'] = splitline[3]
                pricedatum['low'] = splitline[4]
                pricedatum['close'] = splitline[5]
                pricedatum['volume'] = splitline[6]
                pricedatum['adj_close'] = splitline[7].rstrip()
                stocks[symbol]['prices'] = np.append(stocks[symbol]['prices'], pricedatum)

            if report:
                splitline = line.split(",")
                symbol = splitline[0]
                try:
                    stocks[symbol]
                except KeyError:
                    stocks[symbol] = {}
                try:
                    stocks[symbol]['reports']
                except KeyError:
                    stocks[symbol]['reports'] = np.array([])
                reportdatum = {}
                reportdatum['end_date']     = splitline[1]
#                reportdatum['amend']        = splitline[2]
#                reportdatum['period']       = splitline[3]
#                reportdatum['period_focus'] = splitline[4]
#                reportdatum['fiscal_year']  = splitline[5]
#                reportdatum['doc_type']     = splitline[6]
                reportdatum['revenues']     = splitline[7]
#                reportdatum['op_income']    = splitline[8]    # operating income
                reportdatum['net_income']   = splitline[9]   # net income
#                reportdatum['eps_basic']    = splitline[10]  # basic earnings per share
#                reportdatum['eps_diluted']  = splitline[11]  # diluted earnings per share
#                reportdatum['dividend']     = splitline[12]
                reportdatum['assets']       = splitline[13]
#                reportdatum['cur_assets']
#                reportdatum['cur_liab']
#                reportdatum['cash']

                stocks[symbol]['reports'] = np.append(stocks[symbol]['reports'], reportdatum)

            count += 1

"""
                    Clustering measure of similarity

https://www.cs.princeton.edu/sites/default/files/uploads/karina_marvin.pdf
(Notably, sec. 4.1)

A measure of similarity should be selected to cluster similar candidates
together. From the reasoning provided in the paper referred above, the
following parameters should be chosen as clusterable measures.

Ratio of revenues to assets
Ratio of net income to assets
(perhaps a weighted average of the two?)


"""

# trim the fat
for symbol in stocks.keys():
    try:
        stocks[symbol]['prices']
    except KeyError:
        del stocks[symbol]
        continue

    if stocks[symbol]['prices'][0]['volume'] == 'volume':
        del stocks[symbol]
        continue

    stocks[symbol]['prices'] = np.unique(stocks[symbol]['prices'])
    stocks[symbol]['prices'] = np.array(sorted(stocks[symbol]['prices'], key=lambda k: epoch3(k['date'])))

# create some sort of measure of similarity
# in this case, (revenues / assets) and (netincome / assets) are analyzed,
# along with the rate of change of these variables
for symbol in stocks.keys():
    times = np.array([])
    revenues_assets = np.array([])
    netincome_assets = np.array([])

    if 'reports' in stocks[symbol].keys():
        for report in stocks[symbol]['reports']:
            try:
                t = epoch3(report['end_date'])
                revOverAsset        = (float(report['revenues'])   / float(report['assets']))
                netincomeOverAsset  = (float(report['net_income']) / float(report['assets']))
            except ValueError:
                continue

            if len(times) == 0 or t > times[-1]:
                times = np.append(times, t)
                revenues_assets = np.append(revenues_assets, revOverAsset)
                netincome_assets = np.append(netincome_assets, netincomeOverAsset)

        stocks[symbol]['times'] = times
        stocks[symbol]['revenues_assets'] = revenues_assets
        stocks[symbol]['netincome_assets'] = netincome_assets

    try:
        stocks[symbol]['times'][0]
        stocks[symbol]['prices']
    except:
        del stocks[symbol]
        continue

    ti = stocks[symbol]['times']
    ra = stocks[symbol]['revenues_assets']
    nia = stocks[symbol]['netincome_assets']

    if len(ti) == 1:
        del stocks[symbol]
        continue

    # average rate of change in the value of revenues over assets --> deltaRA
    stocks[symbol]['deltaRA'] = [
            (ra[i + 1] - ra[i]) * 100000 /
            ((ti[i+1] - ti[i]) / 60. / 60. / 24.)
            for i in xrange(len(ra) - 1)]

    stocks[symbol]['avgDeltaRA'] = (sum(stocks[symbol]['deltaRA']) /
                                    len(stocks[symbol]['deltaRA']))

    #average rate of change in value of netincome over assets --? deltaNIA
    stocks[symbol]['deltaNIA'] = [
            (nia[i + 1] - nia[i]) * 1000000000000 /
            ((ti[i+1] - ti[i]) / 60. / 60. / 24.)
            for i in xrange(len(nia) - 1)]

    stocks[symbol]['avgDeltaNIA'] = (sum(stocks[symbol]['deltaNIA']) /
                                     len(stocks[symbol]['deltaNIA']))


# time to cluster!
sortedKeys = np.array(sorted(stocks.keys()))
avgDeltaRAs = [stocks[key]['avgDeltaRA'] for key in sortedKeys]
avgDeltaNIAs = [stocks[key]['avgDeltaNIA'] for key in sortedKeys]

clusterer = cluster(avgDeltaRAs, avgDeltaNIAs, minClustSize=5, minSamples=2)


# show some results for Apple Inc
ix = np.where(sortedKeys == 'AAPL')[0][0]
clusterLabel = clusterer.labels_[ix]
matchinglabels = np.where(clusterer.labels_ == clusterLabel)[0]

results = sortedKeys[matchinglabels]

for result in results:
    print result, "\t", symbolsdict[result]

