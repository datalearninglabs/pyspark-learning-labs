import collections
from pyspark import SparkConf, SparkContext
import os

os.environ["PYSPARK_DRIVER_PATH"] = "python"
os.environ["PYSPARK_PYTHON"] = "python"

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/Projects/spark-projects/data/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
