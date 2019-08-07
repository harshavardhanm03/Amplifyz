# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 10:35:33 2019

@author: harsh
"""

from  pyspark.sql import SparkSession,SQLContext
from pyspark.sql import functions as f
import boto3
from pyspark.sql.functions import regexp_replace, trim, lower
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class BatchProcessor(object):
    def __int__(self):
        self.spark = SparkSession.builder.master("spark://ec2-34-206-0-125.compute-1.amazonaws.com:7077").appName("Amazn-Reviews-insights").config("spark.executor.memory", "6gb").getOrCreate()
        self.sqlContext=SQLContext(self.spark)
        
    def sql_read(self):
        departments=[]
        s3= boto3.client('s3')
        response = s3.list_objects_v2(Bucket='amazonreviewsinsight', Delimiter='/')
        obj=response.get('CommonPrefixes')
        for obj in response.get('CommonPrefixes'):
            department = str(obj.get('Prefix')).replace("product_category=","")
            departments.append(department)
        for i in range(0,len(departments)):
            reviews = self.sqlContext.read.parquet('s3a://amazonreviewsinsight/product_category='+departments[i])
        return reviews
        
    def cleaned_reviews(reviews):
        #delete the unwanted columns from the initial dataframe
        reviews=reviews.drop('market_place','product_id','customer_id','review_id','product_parent','vine','review_headline')
        reviews=reviews.filter(reviews.marketplace=='US')
        reviews=reviews.withColumn("product_title_no_punc",lower(trim(regexp_replace('product_title','[^A-Za-z0-9 ]+',''))))
        cleaned_reviews=reviews.drop("product_title")  
        return cleaned_reviews

    
    def aggregate_job(cleaned_reviews):
        #Aggregated the avg rating for eaach and every product on daily basis
        #Removed the punctuation marks from the review text and product name
        #The review_text column is used for doing the review analysis.
        
        cleaned_reviews=cleaned_reviews.groupby('product_title','review_date').agg(f.mean('star_rating').alias('avg_star_rating_daily'),f.count('product_title').alias('no_of_purchases'),f.sum('helpful_votes').alias('helpful_votes_in_day'),f.sum('total_votes').alias('total_votes_in_day'),f.collect_list('review_body').alias("daily_text_review"))
        cleaned_reviews=cleaned_reviews.withColumn("daily_reviews",f.concat_ws(",","daily_text_review"))
        cleaned_reviews=cleaned_reviews.withColumn("reviews_text_no_punc",lower(trim(regexp_replace('product_title','[^A-Za-z0-9 ]+',''))))
        cleaned_reviews=cleaned_reviews.drop("daily_text_reviews")
        return cleaned_reviews
    
    def sentiment_analyzer(cleaned_reviews):
        #Identify the polarity of the review and take the compunded postive reviews count
        analyser = SentimentIntensityAnalyzer()
        sentences=cleaned_reviews.groupby('product_title','review_date').f.concat_ws(", ", f.collect_list(cleaned_reviews['daily_reviews']))
        sentences.withColumn(analyser.polarity_scores(sentences))
        return sentences
        
        
            
        
    def write_df(cleaned_reviews):
        #Writing back the dataframe into a new bucket
        cleaned_reviews.coalesce(1).write.option("header", "true").csv("s3a://amazonreviewsanalysis/")
    
    def write_sentiments(sentences):        
        sentences.coalesce(1).write.option("header", "true").csv("s3a://amazonreviews_scores/")
        
        
    
    def stop_spark(self):
        self.spark.stop()
     


if __name__=="__main__":
    sparkjob = BatchProcessor()
    reviews=sparkjob.sql_read()
    cleaned_reviews=sparkjob.cleaned_reviews(reviews)
    aggregated_reviews=sparkjob.aggregate_job(cleaned_reviews)
    sentence_aggregated=sparkjob.sentiment_analyzer(cleaned_reviews)
    sparkjob.write_df(cleaned_reviews)
    sparkjob.write_sentiments(sentence_aggregated)
    sparkjob.write(aggregated_reviews)
    print('Done')
