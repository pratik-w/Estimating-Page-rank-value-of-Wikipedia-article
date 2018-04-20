
from __future__ import print_function

import re
import sys
from operator import add

from pyspark import SparkContext


def pagerank_value(line_input,url_rank):
    """Calculates URL contributions to the rank of other URLs."""
    line_input=line_input.split(' ')
    no_line = len(line_input)
    for url in line_input:
        yield (url, url_rank / no_line)


def get_tuple(line):
    """Parses a urls pair string into urls pair."""
    
    parts = line.split(': ')
    return parts[0], parts[1]
    
def get_tuple2(line):
    """Parses a urls pair string into urls pair."""
    x = line.split(",")
    return (int(x[0]), x[1])
    


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)

    print("""WARN: This is a naive implementation of PageRank and is
          given as an example! Please refer to PageRank implementation provided by graphx""",
          file=sys.stderr)

    # Initialize the spark context.
    sc = SparkContext(appName="PythonPageRank")

    get_lines_from_file = sc.textFile(sys.argv[1], 1)

    # Loads all URLs from input file and initialize their neighbors.
    init_links = get_lines_from_file.map(lambda line: get_tuple(line))

    #this is a map transformation on lines. separate them into a tuple. 0.00000017 is value of (1/n) 
    init_ranks = init_links.map(lambda url_neighbors: (url_neighbors[0], 0.00000017))				

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        #This function computes how much pagerank score a node receives from its neighbor
        combine = init_links.join(init_ranks).flatMap(
            lambda lis_url_rank: pagerank_value(lis_url_rank[1][0], lis_url_rank[1][1]))

        # sum up the scores received by a node from different neighbors, taken into account the damping factor 
        init_ranks = combine.reduceByKey(add).mapValues(lambda init_ranks: init_ranks* 0.85 + 0.15)  # 0.85*0.15 is taxation factor used for calculation
	final_rank=init_ranks.map(lambda init_ranks_line: (int(init_ranks_line[0]),init_ranks_line[1]))
    
    
    #Get text file and init spark context & form tuple
    title_withindex=sc.textFile('/sampledata/finaltitle.txt',1)    
    title_tuple=title_withindex.map(lambda line: get_tuple2(line))  
    
    #Join Both RDD for comparing Title and page rank
    joinedRDD = title_tuple.join(final_rank)
    joinRDD_final_sort= joinedRDD.sortBy(lambda x: x[1][1],False)
    
    joinRDD_final_sort.saveAsTextFile("wikemyfilenewrank")
    sc.stop()
