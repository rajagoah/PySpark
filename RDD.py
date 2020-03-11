import pyspark as ps
def print_function(x):
    print(x)

if __name__ == "__main__":
    sc = ps.SparkContext("local", "RDD")
    string = "hell0"

    str_rdd = sc.parallelize(string)

    #parallelizing the string and counting the number of variables in the list
    print(str_rdd.count())

    #returning the elements in an RDD
    print(str_rdd.collect())

    #printing all the elements of an RDD. the foreach function is an action. this action opeartion essentially passes each element of the rdd to the print_function() method
    str_rdd.foreach(print_function)