
import sys
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#Input file 
#rdd1=sc.textFile(sys.argv[1])

rdd1=sc.textFile("shortpagerank.txt")
matrix1=rdd1.map(lambda x: (x.split("\t")[0],x.split("\t")[1]))

outlinks=matrix1.map(lambda (x,y): (x,int(1))).reduceByKey(lambda x, y: x + y)
outlinks.collect()
n=outlinks.distinct().count()


adjMatrix=matrix1.join(outlinks).map(lambda (a,(b,c)):((int(a),int(b)),float(1.0/c)))
adjMatrix.collect()

list=range(1,n+1)
vec=sc.parallelize(list).map(lambda x: (x,float(1.0/n)))
vec.collect()


#multiply adj matrix ((i,j), val) with vec (j, val)
adjMatrix.collect()

#One time transpose step
adjMatrix=adjMatrix.map(lambda ((a,b),c):((b,a),c))

for i in range(100):
	print "Running Iteration "+str(i+1)
	join1=adjMatrix.map(lambda ((a,b),c):(b,(a,c))).join(vec)
	#join1.collect()
	vec=join1.map(lambda (j,((i, a),b)): (i, a*b)).reduceByKey(lambda x,y:x+y)
	vec.collect()


'''
#Assumption that the node numbers don't start from anywhere randomly because I am using an array with size max





#find max from 1st and 2nd column, that would be number of nodes
# or find unique entries but then there wont; be one to one mapping
# .reduce(lambda x,y: x[0]>x[1])
'''
