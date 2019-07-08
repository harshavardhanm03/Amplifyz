# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 21:46:41 2019

@author: harsh
"""

import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor


connection = psycopg2.connect(user = "harsha",password = "Harsha9652#",host = "amazon.cpe3nnjoxfg1.us-east-1.redshift.amazonaws.com",port ="5439",database = "amazon")
cur = connection.cursor(cursor_factory=RealDictCursor)

cur.execute("""select 
            product_title_no_punc as Product_Name,
            review_date as Review_Date,
            avg_star_rating_daily as Avg_Star_Rating,
            reviews_no_punc as review_text
            from product_trends""")
df=pd.DataFrame(cur.fetchall())

df['review_text']

text="".join(df['review_text'])

#print(text)
df.columns

import nltk
from nltk.tokenize import word_tokenize
from collections import Counter
from nltk.util import ngrams

tokens = nltk.word_tokenize(text)
bigrams = ngrams(tokens,2)

sorted(bigrams,key=lambda x: x[1])


type(bigrams)





from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

import matplotlib.pyplot as plt

text="".join(df['review_text'][10])

wordcloud = WordCloud().generate(text)
plot=plt.imshow(wordcloud, interpolation='bilinear')

return plot


df['review_text']



def update_figure(product):
    import plotly.plotly as py
    import plotly.tools as tls
	filtered_df=df[df['product_name']=='antennas direct']
	text="".join(filtered_df['review_text'])
	wordcloud = WordCloud().generate(text)
	#x=filtered_df['review_date'].sort_values()
	#y=filtered_df['avg_star_rating_daily']
	plot=plt.imshow(wordcloud, interpolation='bilinear')
	
	return  py.iplot(plot)
	



wordcloud = WordCloud().generate(text)