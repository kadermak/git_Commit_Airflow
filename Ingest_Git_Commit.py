# Author: Kader 
# Task : read from api, output commits heatmap to text file
# DAG to run every 30 mins
#datetime
from datetime import timedelta, datetime
# The DAG object
from airflow import DAG
# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from collections import defaultdict
# imports
import sys
import requests
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns

# initializing the default arguments
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 8, 8),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
ingest_Git_Commit_dag = DAG('ingest_Git_Commit_dag',
                            default_args=default_args,
                            description='ingest Git Commit DAG',
                            schedule_interval='2 * * * *', 
                            catchup=False,
                            tags=['Git_Commit', 'ingest_Git_Commit']
)

# python callable function
def ingest_Git_Commits():
    print("Start Data Pull from URL:")
        
    # GitHub API endpoint for commits
    repo_owner = "apache"
    repo_name = "airflow"
    api_endpoint = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits"

    # Calculate the date 6 months ago from today
    six_months_ago = datetime.now() - timedelta(days=180)
    six_months_ago_str = six_months_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Parameters for the GitHub API request
    params = {
        "since": six_months_ago_str,
        "per_page": 100,  # Number of commits per page (adjust as needed)
    }

    # Dictionary to store commit timestamps for heatmap
    commit_timestamps = defaultdict(list)
    # Dictionary to store commit counts for each author
    commit_counts = defaultdict(int)
    # Dictionary to store commit streaks for each author
    commit_streaks = defaultdict(int)

    # Make the API request
    response = requests.get(api_endpoint, params=params)

    if response.status_code == 200:
        commits = response.json()

        # Calculate commit timestamps for each author
        for commit in commits:
            author_name = commit['commit']['author']['name']
            commit_date = datetime.strptime(commit['commit']['author']['date'], "%Y-%m-%dT%H:%M:%SZ")
            commit_timestamps[author_name].append(commit_date)
        
        # Create a table format for heatmap data using pandas
        heatmap_data = defaultdict(lambda: [[0] * 8 for _ in range(7)])  # 7 days, 8 blocks

        for author, timestamps in commit_timestamps.items():
            for timestamp in timestamps:
                day_of_week = timestamp.weekday()
                time_block = timestamp.hour // 3
                heatmap_data[author][day_of_week][time_block] += 1

        # Convert heatmap_data to a pandas DataFrame
        days_of_week = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        time_blocks = ['00-03', '03-06', '06-09', '09-12', '12-15', '15-18', '18-21', '21-00']

        rows = []
        for day, day_name in enumerate(days_of_week):
            row = [day_name]
            for time_block in range(8):
                total_commits = sum(heatmap_data[author][day][time_block] for author in heatmap_data)
                row.append(total_commits)
            rows.append(row)
        
        heatmap_df = pd.DataFrame(rows, columns=['Day'] + time_blocks)
    
        # Display the heatmap in a table format
        print("##OUTPUT Heatmap of Commit Activity (Table Format):")
        print(heatmap_df)
        
        '''
        # Create a heatmap visualization using seaborn
        # Print Stats
       
        # Determine the top 5 committers
        top_committers = sorted(commit_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Determine the committer with the longest commit streak
        longest_streak_author = max(commit_streaks, key=commit_streaks.get)
        longest_streak = commit_streaks[longest_streak_author]
        
        # Display the top committers and their commit counts
        print("Top 5 Committers:")
        for i, (author, count) in enumerate(top_committers, start=1):
            print(f"{i}. Author: {author}, Commits: {count}")
        
        print(f"\nCommitter with the Longest Commit Streak:")
        print(f"Author: {longest_streak_author}, Streak: {longest_streak}")
	
        '''
        
        '''
        #Output to file
        file_path = 'C:/Users/maric/testdocker/git_Output.txt'    
        with open(file_path, "w") as file:
            file.write("Output Heatmap of Commit Activity (Table Format):")
            #file.write(heatmap_df)
            heatmap_df.to_csv(file_path, sep='\t', index=False)
            file.close()
        '''    
            
    else:
        print(f"Error: Unable to fetch commits. Status Code: {response.status_code}")
      
    print("End Data Pull:")
    return 'Task Completed'



# Creating first task
start_task = DummyOperator(task_id='start_task', dag=ingest_Git_Commit_dag)

# Creating second task
ingest_Git_Commit_task = PythonOperator(task_id='ingest_Git_Commit_task', python_callable=ingest_Git_Commits, dag=ingest_Git_Commit_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=ingest_Git_Commit_dag)

# Set the order of execution of tasks. 
start_task >> ingest_Git_Commit_task >> end_task