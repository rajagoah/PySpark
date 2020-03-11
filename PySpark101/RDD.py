import pyspark as ps
import operator as op
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

    #using the filter() action
    filtered_rdd = str_rdd.filter(lambda x: 'h' in x)

    print(filtered_rdd.collect())

    #using the map() action
    map_rdd = str_rdd.map(lambda x: (1,x))
    print(map_rdd.collect())

    #using the reduce()
    print(sc.parallelize([1,2,3]).reduce(op.add))

    #using join()
    x = sc.parallelize([("spark","1"),("hadoop","2")])
    y = sc.parallelize([("spark","4"),("hadoop","5")])
    joined = x.join(y)
    print(joined.collect())

