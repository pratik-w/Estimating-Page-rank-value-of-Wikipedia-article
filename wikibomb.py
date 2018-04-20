
from __future__ import print_function

import re
import sys
import urllib

from operator import add

from pyspark import SparkContext

def get_tuple2(line):
    """Parses a urls pair string into urls pair."""
    
    x=line.replace(')','')
    y=x.replace('(','')
    z = y.split(",")
    
    return (z[0],z[1],float(z[2]))
    
def replace_underscore(line):
	a=line.replace('_',' ')
	return (a)
	

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)
   
    
    sc = SparkContext(appName="PythonPageRank")			#Spark context is initialized,
	
    get_lines_from_file = sc.textFile(sys.argv[1], 1)		# Get input file from command line argument
	
    Rdd_without_underscore=get_lines_from_file.map(lambda line: replace_underscore(line))		#get line and replace underscore with space
    
    search_querry = urllib.quote("'" + sys.argv[3]	 + "'")							#get search querry
    if 'National' in  search_querry:
		search='National Park'
    else:
	    print("ERROR. TRY AGAIN LATER")			
    
    Rdd_filter=Rdd_without_underscore.filter(lambda x: search in x)
    Rdd_result=Rdd_filter.map(lambda line: get_tuple2(line)).sortBy(lambda Z: Z[2],False)		#find and sort(descending) 
    
    RDD_top5=Rdd_result.take(5)			#select top 5 result
    print(RDD_top5)   
    
   
    sc.stop()
