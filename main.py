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
android_db_path = f"{project_id}.{dataset}.android"

client = bigquery.Client.from_service_account_json(f"{folder_path}/big-data-analytics-412816-1be796546c90_gitCopy.json")
apple_csv_path = f"{folder_path}/apple.csv"
android_csv_path = f"{folder_path}/android.csv"

# Apple
## Clone the repository
subprocess.run(["git", "clone", "https://github.com/gauthamp10/apple-appstore-apps.git"])
## Change directory to the dataset folder
os.chdir("apple-appstore-apps/dataset")
## Extract the tar.lzma file
subprocess.run(["tar", "-xvf", "appleAppData.json.tar.lzma"])
## Read into DataFrame
apple = pd.read_json("appleAppData.json")

# Android
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
android = pd.read_csv("Google-Playstore-Dataset.csv", header = 0)

### Push datasets into Google BigQuery
# Create 'apple' table in DB
job = client.query(f"DROP TABLE {apple_db_path}").result()
client.create_table(bigquery.Table(apple_db_path))

# Create 'android' table in DB
job = client.query(f"DROP TABLE {android_db_path}").result()
client.create_table(bigquery.Table(android_db_path))

# Save data as CSV files
apple.to_csv(apple_csv_path, header = True)
android.to_csv(android_csv_path, header = True)

# Push data into DB
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    max_bad_records=5,
    source_format=bigquery.SourceFormat.CSV
)
apple_config = client.dataset(dataset).table('apple')
android_config = client.dataset(dataset).table('android')

with open(apple_csv_path, 'rb') as f:
    load_job = client.load_table_from_file(f, apple_config, job_config=job_config)
load_job.result()
with open(android_csv_path, 'rb') as f:
    load_job = client.load_table_from_file(f, android_config, job_config=job_config)
load_job.result()

# Remove CSV files and folder
try:
    os.remove(apple_csv_path)
    os.remove(android_csv_path)
    shutil.rmtree(f"{folder_path}apple-appstore-apps")
except:
    pass

# Create 'dateTime' table in DB
dateTime_csv_path = f"{folder_path}/dateTime.csv"
dateTime_db_path = f"{project_id}.{dataset}.dateTime"
client.create_table(bigquery.Table(dateTime_db_path), exists_ok = True)
current_time = datetime.now(timezone('Asia/Shanghai'))
timestamp_string = current_time.isoformat()
dt = datetime.strptime(timestamp_string, '%Y-%m-%dT%H:%M:%S.%f%z')
date_time_str = dt.strftime('%d-%m-%Y %H:%M:%S')  # Date and time
time_zone = dt.strftime('%z')  # Time zone
output = f"Date and Time: {date_time_str}; Time zone: {time_zone}"
dateTime_df = pd.DataFrame(data = [output], columns = ['dateTime'])
dateTime_df.to_csv(f"{folder_path}/dateTime.csv", header = True)
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV,
)
dateTime_config = client.dataset(dataset).table('dateTime')
with open(dateTime_csv_path, 'rb') as f:
    load_job = client.load_table_from_file(f, dateTime_config, job_config=job_config)
load_job.result()
# Remove CSV file
try:
    os.remove(dateTime_csv_path)
except:
    pass

# apple_query = f"""
#     SELECT *
#     FROM {apple_db_path}
# """
# apple_df = read_gbq(apple_query, project_id)

# android_query = f"""
#     SELECT *
#     FROM {android_db_path}
# """
# android_df = read_gbq(android_query, project_id)