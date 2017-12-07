# Stock Market Analyzer #
An experiment in cluster analysis on stock market data for fun and profit.
Some notes for myself to refer to later are included below

Author:     Matt Smith
Date:       2017-12-07

```
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
```

Some stats on the dataset:
    + 9.2m datapoints
    + Data collected from 2015-03-24 to 2017-03-31 (3 years of data)
    + Represents US Stock data
	    

### Clustering measure of similarity ###

https://www.cs.princeton.edu/sites/default/files/uploads/karina_marvin.pdf
(Notably, sec. 4.1)

A measure of similarity should be selected to cluster similar candidates 
together. From the reasoning provided in the paper referred above, the 
following parameters should be chosen as clusterable measures.

  *Ratio of revenues to assets
  *Ratio of net income to assets

(perhaps a weighted average of the two?)
		    
