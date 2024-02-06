# Import libraries and datasets
import os
import subprocess
import glob
import shutil
import pandas as pd
from google.cloud import bigquery
from pandas_gbq import read_gbq
from datetime import datetime
from pytz import timezone
folder_path = os.getcwd().replace("\\", "/")

# Hard-coded variables
project_id = "big-data-analytics-412816"
dataset = "practice_project"
apple_db_path = f"{project_id}.{dataset}.apple"
google_db_path = f"{project_id}.{dataset}.google"

# client = bigquery.Client.from_service_account_json(f"{folder_path}/dataSources/big-data-analytics-412816-1be796546c90.json")
client = bigquery.Client.from_service_account_json(os.environ["GOOGLEAPI"])
apple_csv_path = f"{folder_path}/apple.csv"
google_csv_path = f"{folder_path}/google.csv"

# Apple
## Clone the repository
subprocess.run(["git", "clone", "https://github.com/gauthamp10/apple-appstore-apps.git"])
## Change directory to the dataset folder
os.chdir("apple-appstore-apps/dataset")
## Extract the tar.lzma file
subprocess.run(["tar", "-xvf", "appleAppData.json.tar.lzma"])
## Read into DataFrame
apple = pd.read_json("appleAppData.json")

# Google
## Clone the repository
subprocess.run(["git", "clone", "https://github.com/gauthamp10/Google-Playstore-Dataset.git"])
## Change directory to the dataset folder
os.chdir("Google-Playstore-Dataset/dataset")
## Extract all .tar.gz files
for f in os.listdir():
    if f.endswith(".tar.gz"):
        subprocess.run(["tar", "-xvf", f])
combined_csv = "Google-Playstore-Dataset.csv"
with open(combined_csv, "wb") as outfile:
    for csvfile in glob.glob("Part?.csv"):
        with open(csvfile, "rb") as infile:
            outfile.write(infile.read())
## Read into DataFrame
google = pd.read_csv("Google-Playstore-Dataset.csv", header = 0) # low_memory = False

# Create tables into Google BigQuery
## Create 'apple' table in DB
job = client.query(f"DELETE FROM {apple_db_path} WHERE TRUE").result()
client.create_table(bigquery.Table(apple_db_path), exists_ok = True)
## Create 'google' table in DB
job = client.query(f"DELETE FROM {google_db_path} WHERE TRUE").result()
client.create_table(bigquery.Table(google_db_path), exists_ok = True)

# Save data as CSV files
apple.columns = [name.replace(" ", "_") for name in apple.columns]
apple.to_csv(apple_csv_path, header = True, index = False)
google.columns = [name.replace(" ", "_") for name in google.columns]
google.to_csv(google_csv_path, header = True, index = False)

# Push data into DB
apple_job_config = bigquery.LoadJobConfig(
    autodetect=True,
    max_bad_records=5,
    source_format=bigquery.SourceFormat.CSV
)
apple_config = client.dataset(dataset).table('apple')
with open(apple_csv_path, 'rb') as f:
    apple_load_job = client.load_table_from_file(f, apple_config, job_config=apple_job_config)
apple_load_job.result()

google_job_config = bigquery.LoadJobConfig(
    autodetect=False,
    skip_leading_rows=1,
    max_bad_records=5,
    source_format=bigquery.SourceFormat.CSV
)
google_config = client.dataset(dataset).table('google')
with open(google_csv_path, 'rb') as f:
    google_load_job = client.load_table_from_file(f, google_config, job_config=google_job_config)
google_load_job.result()

# Create 'dateTime' table in DB
dateTime_csv_path = f"{folder_path}/dateTime.csv"
dateTime_db_path = f"{project_id}.{dataset}.dateTime"
job = client.query(f"DELETE FROM {dateTime_db_path} WHERE TRUE").result()
client.create_table(bigquery.Table(dateTime_db_path), exists_ok = True)
current_time = datetime.now(timezone('Asia/Shanghai'))
timestamp_string = current_time.isoformat()
dt = datetime.strptime(timestamp_string, '%Y-%m-%dT%H:%M:%S.%f%z')
date_time_str = dt.strftime('%d-%m-%Y %H:%M:%S')  # Date and time
time_zone = dt.strftime('%z')  # Time zone
output = f"{date_time_str}; GMT+{time_zone[2]} (SGT)"
dateTime_df = pd.DataFrame(data = [output], columns = ['dateTime'])
dateTime_df.to_csv(f"{folder_path}/dateTime.csv", header = True, index = False)
dateTime_job_config = bigquery.LoadJobConfig(
    autodetect=True,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
dateTime_config = client.dataset(dataset).table('dateTime')
with open(dateTime_csv_path, 'rb') as f:
    dateTime_load_job = client.load_table_from_file(f, dateTime_config, job_config=dateTime_job_config)
dateTime_load_job.result()

## Remove CSV files and folder
try:
    os.remove(apple_csv_path)
    os.remove(google_csv_path)
    os.remove(dateTime_csv_path)
    shutil.rmtree(f"{folder_path}apple-appstore-apps")
except:
    pass

# apple_query = f"""
#     SELECT *
#     FROM {apple_db_path}
# """
# apple_df = read_gbq(apple_query, project_id)

# google_query = f"""
#     SELECT *
#     FROM {google_db_path}
# """
# google_df = read_gbq(google_query, project_id)