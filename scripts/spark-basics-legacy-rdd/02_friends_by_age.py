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
    .setMaster("local") \
    .setAppName("FriendsByAge")


# Create SparkContext, which is the entry point for interacting with Spark
# It allows us to create RDDs and execute Spark operations
sc = SparkContext(conf = conf)

# Parse a CSV record and extract the age and number of friends.
# Returns a tuple in the format: (Age, Number of Friends)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# Determine the root project directory dynamically
# __file__ represents the current Python file location
# parent.parent.parent moves three levels up to reach the project root
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent


# Define the path to the fake friends dataset
# The dataset contains data in the format:
# Serial_Number Name  Age No.of Friends
data_file_path = PROJECT_PATH / "data" / "fakefriends.csv"

# Load the fake friends dataset into an RDD
# Each line in the CSV file becomes one element in the RDD
lines = sc.textFile(str(data_file_path))

# Parse each record to extract only the required fields: Age & Number of Friends
# Example
# input: 0,Will,33,385 & Output: (33, 385)
#
# Each record is transformed into a key-value pair where: Key -> Age & Value -> Number of Friends
rdd = lines.map(parseLine)

# Convert each value into a tuple of: (Number of Friends, 1)
# Example: (33, 385) becomes  (33, (385, 1))
#
# reduceByKey() then combines all records having the same age by:
# - Summing the total number of friends
# - Counting the total number of people#
# Example:
# (33, (385,1)), (33, (250,1)), (33, (400,1)) becomes (33, (1035,3))
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))

# Calculate the average number of friends for each age
# Average = Total Friends / Number of People
# Example: (33, (1035,3)) becomes (33, 345.0)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# Collect the distributed RDD back to the driver program
# This converts the RDD into a local Python list for display
results = averagesByAge.collect()

# Print the average number of friends for each age
# Example output: (18, 343.38) (19, 213.27) ...
for age, average in results:
    print((age, f"{average:.2f}"))

# Stop SparkContext to release Spark resources
sc.stop()






