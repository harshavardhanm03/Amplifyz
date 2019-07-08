# Amplifyz

[![N|Solid](https://cldup.com/dTxpPi9lDf.thumb.png)](https://nodesource.com/products/nsolid)

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

# Motivation
Millions of product owners sell their products through amazon. Tracking the reviews and ratings of each product is very cumbersome. Amplifyz helps to track each and every product with help of an interactive dashboard application.

# Getting Started
Data is publicly available at [https://registry.opendata.aws/amazon-reviews/]
17 Years (1999-2015)
50 GB
9M+ Products
20M+ Users
130M+ Reviews

# Data Pipeline

![Overview of Data Pipeline](https://github.com/harshavardhanm03/Amplifyz/blob/master/images/datapipline.PNG)


Data Flow from various tools as follows:
  - Initially reviews are stored in an S3 Bucket.
  - Spark loads all reviews in parquet format into memory.
  - Spark groups reviews by product on day basis and computes aggregation such as average rating,reviews, total votes and helpful votes.
  - Spark writes this data back into S3 in format of CSV to a new bucket.
  - Tables are created in Redshift with similar schema as that of CSV files.
  - SQL/T copies all the files in S3 into reshift.
  - Dash is used for  Web applications.
  
  
### Presentation link
 Link to the presentation :   [Ampliyfz](https://docs.google.com/presentation/d/160TNlNY0xZC9PjaZhpJ-dyUDK5l86CZv35HrqPLn1J8/edit#slide=id.p1)
