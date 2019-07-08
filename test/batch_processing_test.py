# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 17:31:09 2019

@author: harsh
"""

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace, trim, col, lower
import boto3
spark = SparkSession.builder.master("spark://ec2-34-206-0-125.compute-1.amazonaws.com:7077").appName("amazon-insights").config("spark.executor.memory", "6gb").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)
departments=[]
s3= boto3.client('s3')
response = s3.list_objects_v2(Bucket='amazonreviewsinsight', Delimiter='/')
obj=response.get('CommonPrefixes')
for obj in response.get('CommonPrefixes'):
     department = str(obj.get('Prefix')).replace("product_category=", "")
     departments.append(department)

reviews = sqlContext.read.parquet('s3a://amazonreviewsinsight/product_category=Electronics/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')
reviews = sqlContext.read.parquet('s3a://amazonreviewsinsight/product_category='+departments[0])
reviews=reviews.filter(reviews.marketplace=='US')
reviews=reviews.drop('market_place','product_id','customer_id','review_id','product_parent','vine','review_headline')
reviews=reviews.groupby('product_title','review_date').agg(f.mean('star_rating').alias('avg_star_rating_daily'),f.count('product_title').alias('no_of_purchases'),f.sum('helpful_votes').alias('helpful_votes_in_day'),f.sum('total_votes').alias('total_votes_in_day'),f.collect_list('review_body').alias("daily_text_review"))
reviews=reviews.withColumn("reviews_no_punc",lower(trim(regexp_replace('daily_reviews','[^A-Za-z0-9 ]+',''))))