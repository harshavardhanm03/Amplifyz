import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import psycopg2
import datetime as dt
from datetime import datetime
from collections import Counter
import nltk
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from psycopg2.extras import RealDictCursor
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import plot
from dash.dependencies import Input, Output
import nltk
from nltk.tokenize import word_tokenize
from collections import Counter
from nltk.util import ngrams
from nltk.corpus import stopwords


#from plotly_wordcloud import plotly_wordcloud as pwc







app = dash.Dash()
import pandas as pd

#Create a connection to redshift
connection = psycopg2.connect(user = "harsha",password = "Harsha9652#",host = "amazon.cpe3nnjoxfg1.us-east-1.redshift.amazonaws.com",port ="5439",database = "amazon")
cur = connection.cursor(cursor_factory=RealDictCursor)

#Select the date using redshift query
cur.execute("""select 
			product_title_no_punc as product_name,
			count(product_title_no_punc),
			avg(avg_star_rating_daily) as avg_rating,
			max(review_date) as review_date,
			sum(helpful_votes_in_day) as helpful_votes_week
			from product_trends2
			group by
			product_title_no_punc,extract(year from review_date),extract(month from review_date),extract(week from review_date)
			order by review_date"""
			)		
			
			
df=pd.DataFrame(cur.fetchall())
products=df['product_name'].unique()


#CSS and HTML STYLING and LAYOUT DEVELOPING
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}
app.layout = html.Div(
	style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Amplify',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.H3(
			children='Track Product Ratings',
		style={
			'textAlign': 'center',
			'color': colors['text']
			}),

    dcc.Dropdown(
                    id='product_id',
                    options=[{'label': i, 'value': i} for i in
                             products],
					placeholder="Select product",
                    value=[i for i in df['product_name']],
					#value="antennas direct",
                    className='Product'
                ),	
	html.Div([
        html.Div([
            html.H3('Avg Rating Trend'),
            dcc.Graph(id='g1')
        ], className="six columns"),

        html.Div([
            html.H3('Sentiment Trend'),
            dcc.Graph(id='g2')
        ], className="six columns"),
    ], className="row")
						

		])

		
#Call back to figure 1
@app.callback(
	Output(component_id='g1', component_property='figure'),
	[Input(component_id='product_id',component_property='value')
	 #Input(component_id='date-picker-range',component_property=start_date),
	 #Input(component_id='date-picker-range',component_property=end_date)	 
	]
			)	
			
def update_figure(product):
	filtered_df=df[df['product_name']==product]
	#x=filtered_df['review_date'].sort_values()
	#y=filtered_df['avg_star_rating_daily']
	return  {

        'data': [{

            'x': filtered_df['review_date'].sort_values(),

            'y': filtered_df['avg_rating']

        }],

        'layout': {'margin': {'l': 40, 'r': 0, 't': 20, 'b': 30}}

    }
	

@app.callback(
	Output(component_id='g2', component_property='figure'),
	[Input(component_id='product_id',component_property='value')
	 #Input(component_id='date-picker-range',component_property=start_date),
	 #Input(component_id='date-picker-range',component_property=end_date)	 
	]
			)	
			
def update_figure2(product):
	filtered_df=df[df['product_name']==product]
	#x=filtered_df['review_date'].sort_values()
	#y=filtered_df[' _star_rating_daily']
	return  {

        'data': [{

            'x': filtered_df['review_date'].sort_values(),

            'y': filtered_df['helpful_votes_week']

        }],

        'layout': {'margin': {'l': 40, 'r': 0, 't': 20, 'b': 30}}

    }


if __name__ == '__main__':
    app.run_server(debug=False)




