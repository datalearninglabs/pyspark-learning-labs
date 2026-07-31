# Spark Basics - Legacy RDD API

This folder contains PySpark programs demonstrating the fundamentals of Apache Spark using the **Legacy RDD (Resilient Distributed Dataset) API**.

The programs focus on understanding core Spark concepts such as:
- Creating Spark applications
- Working with RDDs
- Applying transformations and actions
- Processing and analyzing distributed datasets

---

# Program 01: Ratings Histogram Analysis

## Python Script

```
01_ratings_counter.py
```

## Overview

This program demonstrates how to use PySpark RDD operations to analyze movie rating data from the **MovieLens 100K dataset**.

The application reads the ratings dataset, extracts the rating values, and calculates the frequency of each rating using Spark's RDD processing capabilities.

The output provides a histogram showing how many times each rating value (1 to 5 stars) appears in the dataset.

## Spark Concepts Demonstrated

- Creating a SparkContext
- Reading data into an RDD
- Using `map()` transformation
- Using `countByValue()` action
- Processing and aggregating distributed data

## Technologies Used

- Python
- Apache Spark
- PySpark RDD API

# Program 02: Average Friends by Age Analysis

## Python Script
`02_friends_by_age.py`

## Dataset
`fakefriends.csv`

## Overview

This program demonstrates how to use **PySpark Key-Value Pair RDDs** to analyze a social network dataset and calculate the **average number of friends for each age**.

The application reads a CSV dataset, extracts the age and number of friends for each individual, aggregates the total number of friends and the total number of people for each age, and computes the average using Spark's distributed RDD transformations.

The output displays the average number of friends for every age present in the dataset.

## Spark Concepts Demonstrated

- Creating a SparkContext
- Reading data into an RDD
- Parsing CSV data using `map()`
- Creating Key-Value Pair RDDs
- Using `mapValues()` to transform values while preserving keys
- Aggregating data with `reduceByKey()`
- Using tuples (composite values) for distributed aggregation
- Calculating averages from aggregated data
- Collecting and displaying results using `collect()`

## Technologies Used

- Python
- Apache Spark
- PySpark RDD API