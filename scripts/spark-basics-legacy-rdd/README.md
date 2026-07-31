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