import pyspark as ps

sc = ps.SparkContext("local", "RDD")
string = "hell"

str_rdd = sc.parallelize(string)

#parallelizing the string and counting the number of variables in the list
print(str_rdd.count())

#returning the elements in an RDD
print(str_rdd.collect())

#printing all the elements of an RDD
def f(x):
    print(x)
    print(str_rdd.foreach(f))