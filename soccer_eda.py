import sqlite3
import pandas as pd

# Set up connection to sqlite database
conn = sqlite3.connect('/Users/aimaanwar/Downloads/project_2/database.sqlite')
cursor = conn.cursor()

# Execute SQL queries using cursor.execute(); create pandas DataFrames for each table
match_df = pd.read_sql_query("SELECT * FROM Match", conn)
team_df = pd.read_sql_query("SELECT * FROM Team", conn)
player_df = pd.read_sql_query("SELECT * FROM Player", conn)
player_att_df = pd.read_sql_query("SELECT * FROM Player Attributes", conn)
country_df = pd.read_sql_query("SELECT * FROM Country", conn)
team_att_df = pd.read_sql_query("SELECT * FROM Team Attributes", conn)
league_df = pd.read_sql_query("SELECT * FROM League", conn)

# Close the connection
conn.close()

'''1. Compare team performance at home and away'''
# Number of teams
unique_teams = team_df['team_api_id'].nunique()
print("Number of unique teams in the dataset:", unique_teams)

# Calculate points earned by each team at home and away
winning_home_matches = match_df[match_df['home_team_goal'] > match_df['away_team_goal']]
winning_away_matches = match_df[match_df['away_team_goal'] > match_df['home_team_goal']]

home_wins = winning_home_matches.groupby('home_team_api_id')['id'].count()
away_wins = winning_away_matches.groupby('away_team_api_id')['id'].count()
home_wins = home_wins.reindex(away_wins.index, fill_value=0)

more_home_wins = home_wins[home_wins > away_wins]
more_away_wins = home_wins[home_wins < away_wins]
equal_wins = home_wins[home_wins == away_wins]
num_more_home_wins = len(more_home_wins)
num_more_away_wins = len(more_away_wins)
num_equal_wins = len(equal_wins)
percent_more_home_wins = num_more_home_wins / unique_teams * 100
percent_more_away_wins = num_more_away_wins / unique_teams * 100
percent_equal_wins = num_equal_wins / unique_teams * 100

print(f"Number of teams with more home wins than away wins: {num_more_home_wins} ({percent_more_home_wins:.2f}%)")
print(f"Number of teams with more away wins than home wins: {num_more_away_wins} ({percent_more_away_wins:.2f}%)")
print(f"Number of teams with equal number of home and away wins: {num_equal_wins} ({percent_equal_wins:.2f}%)")

# Visualize the results using data visualization libraries like Matplotlib or Seaborn
# Draw conclusions based on your analysis

'''2. Did teams improve over time?'''
# Extract necessary information from match_df and create 2 dataframes
home_matches = match_df[['home_team_api_id', 'home_team_goal', 'date']]
away_matches = match_df[['away_team_api_id', 'away_team_goal', 'date']]

# Rename columns in the separate DataFrames and concatenate them into a combined dataframe
home_matches.columns = ['team_api_id', 'goals', 'date']
away_matches.columns = ['team_api_id', 'goals', 'date']
comb_matches = pd.concat([home_matches, away_matches], axis=0)

# Convert the date column to datetime and extract the year from the date
comb_matches['date'] = pd.to_datetime(comb_matches['date'])
comb_matches['year'] = comb_matches['date'].dt.year

# Group by team ID and year, and sum the goals scored
team_goals = comb_matches.groupby(['team_api_id', 'year'])['goals'].sum().reset_index()

# Calculate the median year to split the data into two halves
median_year = team_goals['year'].median()

# Create two DataFrames for the first and second halves
first_half = team_goals[team_goals['year'] <= median_year]
second_half = team_goals[team_goals['year'] > median_year]

# Calculate the total goals for each team in the first and second halves
first_half_goals = first_half.groupby('team_api_id')['goals'].sum().reset_index()
second_half_goals = second_half.groupby('team_api_id')['goals'].sum().reset_index()

# Find the teams that improved in the second half
improved_teams = first_half_goals[first_half_goals['team_api_id'].isin(second_half_goals['team_api_id'])]
improved_teams = improved_teams[improved_teams['goals'].values < second_half_goals[second_half_goals['team_api_id'].isin(improved_teams['team_api_id'])]['goals'].values]['team_api_id'].tolist()

# Calculate the percentage of teams that improved
improvement_percentage = (len(improved_teams) / unique_teams) * 100

# Print the result
print(f"Percentage of teams that improved in the second half: {improvement_percentage:.2f}%")
print("List of improved teams:", improved_teams)


'''3. Did overall performance improve between 2008 and 2016'''

# Filter the team_goals DataFrame to include only data between 2008 and 2016
filtered_team_goals = team_goals[(team_goals['year'] >= 2008) & (team_goals['year'] <= 2016)]

# Group the filtered data by year and calculate the total goals scored in each year
yearly_goals = filtered_team_goals.groupby('year')['goals'].sum()

# Calculate the percentage change in total goals scored from 2008 to 2016
initial_goals = yearly_goals[2008]
final_goals = yearly_goals[2016]
percentage_change = ((final_goals - initial_goals) / initial_goals) * 100

# Check if the overall performance improved
if percentage_change > 0:
    print(f"Overall performance improved by {percentage_change:.2f}% from 2008 to 2016.")
else:
    print(f"Overall performance did not improve from 2008 to 2016.")

'''Are there any correlations between total number of goals and team attributes? Can we find correlation between team attributes?'''

# Merge the two DataFrames based on the team_api_id
merged_df = team_goals.merge(team_att_df, on='team_api_id')

# Select only numeric columns for calculating correlations
numeric_columns = merged_df.select_dtypes(include=['number'])

# Calculate correlations between total goals and team attributes
correlations = numeric_columns.corr()

# Print the correlation matrix
print(correlations)

# # To find correlations between team attributes, you can create a correlation matrix as follows:
# team_attributes_correlations = team_att_df.corr()
# print(team_attributes_correlations)