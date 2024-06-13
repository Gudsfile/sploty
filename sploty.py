import pandas as pd
import numpy as np


input_folder = 'results'
output_folder = 'to_grafana'

# Get all pass data

df1 = pd.read_csv(input_folder + '/my_spotify_data_1/MySpotifyDataTable.csv', sep=',')
df2 = pd.read_csv(input_folder + '/my_spotify_data_2/MySpotifyDataTable.csv', sep=',')
df3 = pd.read_csv(input_folder + '/my_spotify_data_3/MySpotifyDataTable.csv', sep=',')

df = pd.concat([df1, df2, df3], ignore_index=True).loc[:, ~df.columns.str.contains('^Unnamed')]
df = df.drop_duplicates(ignore_index=True)

df.to_csv(input_folder + '/MySpotifyDataTable_full.csv')


# read file
df = pd.read_csv(input_folder + '/MySpotifyDataTable_full.csv', sep=',')

# add column for minutes played
df['minPlayed'] = df['msPlayed'].div(60000).round(2)

# add columns for date
df['year'] = pd.DatetimeIndex(df['endTime']).year.map("{:04}".format)
df['month'] = (pd.DatetimeIndex(df['endTime']).month).map("{:02}".format)
df['month_name'] = pd.DatetimeIndex(df['endTime']).month_name()
df['day'] = pd.DatetimeIndex(df['endTime']).day.map("{:02}".format)
df['hour'] = pd.DatetimeIndex(df['endTime']).hour.map("{:02}".format)
df['minute'] = pd.DatetimeIndex(df['endTime']).minute.map("{:02}".format)

# to csv schema
df['date'] = df['year'] + "-" + df['month'] + "-" + df['day'] + " " + df['hour'] + ":00"
df.groupby(['date','hour'])['msPlayed'].sum().to_csv(output_folder + '/agg_hourly.csv')
df['date'] = df['year'] + "-" + df['month'] + "-" + df['day'] + " " + "01" + ":00"
df.groupby(['date'])['msPlayed'].sum().to_csv(output_folder + '/agg_daily.csv')
df['date'] = df['year'] + "-" + df['month'] + "-" + "01" + " " + "01" + ":00"
df.groupby(['date', 'month_name'])['msPlayed'].sum().to_csv(output_folder + '/agg_monthly.csv')
df['date'] = df['year'] + "-" + "01" + "-" + "01" + " " + "01" + ":00"
df.groupby(['date'])['msPlayed'].sum().to_csv(output_folder + '/agg_yearly.csv')

# to csv top
df['date'] = df['year'] + "-" + df['month'] + "-" + "01" + " " + "01" + ":00"
df.groupby(['date', 'trackName', 'artistName'])['msPlayed'].agg(['count', 'sum']).to_csv(output_folder + '/agg_track_played.csv')

df.dates = df.dates.apply(lambda x: x.date())




"""
df1 = pd.read_csv(input_folder + '/MySpotifyDataTable.csv', sep=',')
df2 = pd.read_csv(input_folder + '/MySpotifyDataTable2.csv', sep=',')
df3 = pd.read_csv(input_folder + '/MySpotifyDataTable3.csv', sep=',')

df = pd.concat([df1, df2], ignore_index=True).loc[:, ~df.columns.str.contains('^Unnamed')]
df = df.drop_duplicates(ignore_index=True)

df.to_csv('results/MySpotifyDataTable_full.csv')

>>> len(df1)
20034
>>> len(df2)
29675
>>> len(df)
49709
>>> len(df)
32204
"""
