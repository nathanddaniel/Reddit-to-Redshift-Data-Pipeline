from airflow import DAG #importing the DAG class, DAG directs the overall workflow of tasks (how they run, and which order)
from datetime import datetime #used to define the start_date for when the DAG start scheduling the runs
from airflow.operators.python import PythonOperator #this operator allows for running Python functions as a task, used later for Reddit extraction

#used for file and path manipulation, modifying sys.path so we can import code into this file
import os 
import sys

#allowing us to import modules outside of the dags/ folder, such as the reddit_pipeline function from pipelines/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 

#default arguments for all tasks within the DAG
default_args = {
    'owner': 'Nathan Daniel', #owner is just meta data
    'start_date': datetime(2025, 6, 4) #start_date sets the first date that Airflow considers for scheduling
}

#generates a timestamp, which will be used to name the Reddit output data file (eg. reddit_250604.json)
file_postfix = datetime.now().strftime("%y%m%d")

#creating the actual DAG object that Airflow will run
dag = DAG(
    dag_id='etl_reddit_pipeline', #dag_id is how it shows up in the UI
    default_args=default_args, #using the default arguments defined earlier
    schedule_intervals='@daily', #this will determine how often the DAG runs
    catchup=False, #this ensures it runs starting from today and not missed days in the past
    tags=['reddit', 'etl', 'pipeline'] #these tags help organize DAGs in the UI
)

#defining a task which runs the reddit_pipeline function
extract = PythonOperator(
    task_id = 'reddit_extraction', #this is the name of the task shown in the UI 
    python_callable=reddit_pipeline, #this is the actual function to call for this task

    #this passes keyword arguments into the reddit_pipeline function
    op_kwargs = {
        'file_name': f'reddit_{file_postfix}', #file_name is what the Reddit data file will be called, unique because of the timestamp appended
        'subreddit': 'artificial', #setting which subreddit to pull data from... I'm interested in AI, so let's pull data form the r/artificial sr
        'time_filter': 'day', 
        'limit': 100 #liminting the number of posts to retrieve
    }
)

#upload to s3