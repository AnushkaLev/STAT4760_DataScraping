import pandas as pd
import math

file1 = "data/raw/raw_bills_broncos_p1.csv"
file2 = "data/raw/raw_bills_broncos_p2.csv"

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# combine datasets
df = pd.concat([df1, df2], ignore_index=True)

# count comments per author
comment_counts = df['author'].value_counts()

# determine number of users in top 5%
total_users = len(comment_counts)
top_n = math.ceil(0.05 * total_users)

# get total comments
total_comments = len(df)

# get comments from top 5% users
top_users = comment_counts.head(top_n)
top_comments = top_users.sum()

# share of total comments
share = top_comments / total_comments

share