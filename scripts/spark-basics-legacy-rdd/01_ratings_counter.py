# Import Python collections module to use OrderedDict for sorting results
import collections

# Import time module to measure execution duration
import time

# Import Spark configuration and SparkContext classes required to create a Spark application
from pyspark import SparkConf, SparkContext

# Import Path to handle file paths in an operating-system-independent way
from pathlib import Path

import os

# Configure Python interpreter paths used by PySpark
# Ensures Spark uses the correct Python executable for driver and worker processes
os.environ["PYSPARK_DRIVER_PATH"] = "python"
os.environ["PYSPARK_PYTHON"] = "python"


# Create Spark configuration:
# - setMaster("local") runs Spark locally on the machine
# - setAppName assigns a name to this Spark application
conf = SparkConf() \
    .setMaster("local[4]") \
    .setAppName("RatingsHistogram")


# Create SparkContext, which is the entry point for interacting with Spark
# It allows us to create RDDs and execute Spark operations
sc = SparkContext(conf=conf)


# Determine the root project directory dynamically
# __file__ represents the current Python file location
# parent.parent.parent moves three levels up to reach the project root
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent


# Define the path to the MovieLens ratings dataset
# The dataset contains user ratings in the format:
# UserID MovieID Rating Timestamp
data_file_path = PROJECT_PATH / "data" / "ml-100k" / "u.data"

print(data_file_path)


# Record the start time before the computation begins
start_time = time.time()

# Load the ratings file into an RDD (Resilient Distributed Dataset)
# Each line in the file becomes one element in the RDD
lines = sc.textFile(str(data_file_path))

# Extract the UserID column (index 0) from each record
user_ids = lines.map(lambda x: x.split()[0])

# Count the number of distinct users
num_users = user_ids.distinct().count()

print(f"Number of distinct users: {num_users}")

# Extract only the rating column from each record
# Index [2] retrieves the rating value (3 in this example)
ratings = lines.map(lambda x: x.split()[2])


# Count how many times each rating value appears
# Example output:
# {
#   '1': 611,
#   '2': 1131,
#   '3': 27145,   
# }
#
# countByValue() performs an aggregation operation on the RDD
result = ratings.countByValue()


# Sort ratings in ascending order so the output is easier to analyze
# OrderedDict preserves the sorted order of the ratings
sortedResults = collections.OrderedDict(sorted(result.items()))


# Display the rating frequency distribution
# Rating  Count
# 1       611
# 2       1131
# ...
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

# Record the end time and calculate total elapsed time
end_time = time.time()
print(f"\n[Timing] Tasks: 4 (local[4]) | Elapsed time: {end_time - start_time:.4f} seconds")

# Stop SparkContext gracefully after completing the job
# This releases Spark resources and closes the application
sc.stop()
