# page-rank in pyspark

Simple page Rank:
File name is hardcoded as pageRankInput.txt So while running, you would have to keep the filename pageRankInput.txt in the same directory.
Matrix1 key is node1,value is the outline
Outlinks rdd is computing how many outlinks are there 
adjMatrix is computed using join (1.0/c makes the sum as one for columns)
Initial vector values are (1/n)
I have used 50 iterations to repetitively do matrix vector multiplication.

Modified page Rank:
Vector values after matrix multiplication are modified to incorporate beta to handle dead ends and spider traps. 
