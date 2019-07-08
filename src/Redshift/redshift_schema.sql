CREATE  TABLE product_trends(
  review_date date,
  avg_star_rating_daily float,
  no_of_purchases integer not null,
  helpful_votes_in_day integer,
  total_votes_in_day integer,
  product_title_no_punc nvarchar(max),  
  reviews_no_punc nvarchar(max) not null
)
