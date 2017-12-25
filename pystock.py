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


datapath = r"/host/Users/mokho_000/Bash/python/stock-market-analyzer/data/pystock-data-gh-pages"


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


def cluster(revenues_assets, netincome_assets, deltaRA, deltaNIA, minClustSize, minSamples):
    """
    Returns a clustering on revenue/time and netincome/time data
    """
    x = zip(revenues_assets, netincome_assets, deltaRA, deltaNIA)
    clusterer = (hdbscan.HDBSCAN(algorithm='prims_kdtree',
                 min_cluster_size=minClustSize,
                 min_samples=minSamples,
                 prediction_data=True,
                 cluster_selection_method='leaf',
                 gen_min_span_tree=False,
                 metric='euclidean').fit(x))
    return clusterer


def read_dataset(datafiles):

    stocks = {'symbols': {},
              'prices' : {},
              'reports': {}}

    for ix, datapath in enumerate(datafiles):

        print("Processing %d/%d\t%s" % (ix, len(datafiles), datapath.split("/")[-1]))

        if memory() > 75:
            print("\nRunning out of memory! Data parsing has been halted.")
            break

        d = gzip.GzipFile(datapath)

        for line in d:
            if ".txt" in line or ".csv" in line:
                datatype = line.split('\x00')#.split(".")
                datatype = filter(None, datatype)[0].split('.')[0]
                continue
            if datatype == 'symbols':
                l = line.rstrip().split('\t')
            else:
                l = line.rstrip().split(',')

            if '\x00' in l: continue

            try:
                stocks[datatype][l[0]]
            except KeyError:
                if datatype == "symbols":
                    stocks[datatype][l[0]] = l[1]
                else:
                    stocks[datatype][l[0]] = np.array([])
            if datatype == 'symbols':
                pass
            elif datatype == 'prices':
                stocks[datatype][l[0]] = np.append(stocks[datatype][l[0]], l[1:])
            elif datatype == 'reports':
                stocks[datatype][l[0]] = np.append(stocks[datatype][l[0]], l[1:])

    return stocks


datafiles = [os.path.join(root, name)
             for root, dirs, files in os.walk(datapath)
             for name in files
             if name.endswith(".tar.gz")]


datafiles = datafiles[3:]  # temporary fix
stocks = read_dataset(datafiles)



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

for symbol in stocks.keys():

    if 'reports' in stocks[symbol].keys():
        for report in stocks[symbol]['reports']:
            try:
                t = epoch3(report['end_date'])
                revOverAsset        = (float(report['revenues'])   / float(report['assets']))
                netincomeOverAsset  = (float(report['net_income']) * float(report['assets']))
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


    stocks[symbol]['avgRA'] = (sum(stocks[symbol]['revenues_assets']) /
                               len(stocks[symbol]['revenues_assets']))

    stocks[symbol]['avgNIA'] = (sum(stocks[symbol]['netincome_assets']) /
                                len(stocks[symbol]['netincome_assets']))


    # average rate of change in the value of revenues over assets --> deltaRA
    stocks[symbol]['deltaRA'] = [
            (ra[i + 1] - ra[i]) * 100000. /
            ((ti[i+1] - ti[i]) / 60. / 60. / 24.)
            for i in xrange(len(ra) - 1)]

    stocks[symbol]['avgDeltaRA'] = (sum(stocks[symbol]['deltaRA']) /
                                    len(stocks[symbol]['deltaRA']))

    #average rate of change in value of netincome over assets --> deltaNIA
    stocks[symbol]['deltaNIA'] = [
            (nia[i + 1] - nia[i]) / 100000. /
            ((ti[i+1] - ti[i]) / 60. / 60. / 24.)
            for i in xrange(len(nia) - 1)]

    stocks[symbol]['avgDeltaNIA'] = (sum(stocks[symbol]['deltaNIA']) /
                                     len(stocks[symbol]['deltaNIA']))


# time to cluster!
sortedKeys = np.array(sorted(stocks.keys()))
avgRAs = [stocks[key]['avgRA'] for key in sortedKeys]
avgNIAs = [stocks[key]['avgNIA'] for key in sortedKeys]
avgDeltaRAs = [stocks[key]['avgDeltaRA'] for key in sortedKeys]
avgDeltaNIAs = [stocks[key]['avgDeltaNIA'] for key in sortedKeys]

clusterer = cluster(avgRAs, avgNIAs, avgDeltaRAs, avgDeltaNIAs, minClustSize=5, minSamples=2)


# show some results
ix = np.where(sortedKeys == 'CSX')[0][0]
clusterLabel = clusterer.labels_[ix]
matchinglabels = np.where(clusterer.labels_ == clusterLabel)[0]

results = sortedKeys[matchinglabels]

for result in results:
    print (result + "\t" + symbolsdict[result] +
           ("\n\t\tAverage revenue / assets:\t\t%.2f" +
            "\n\t\tAverage net income / assets:\t\t%.2f " +
            "\n\t\tAverage delta revenue / assets:\t\t%.2f" +
            "\n\t\tAverage delta net income / assets:\t%.2f\n")
           % (stocks[result]['avgRA'], stocks[result]['avgNIA'],
              stocks[result]['avgDeltaRA'], stocks[result]['avgDeltaNIA']))

