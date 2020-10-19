import os

import awswrangler as wr
import pandas as pd
import requests
from draft_kings.client import contests
from draft_kings.data import Sport
import boto3
from io import StringIO


def get_field(league, n):
    ''' Get the field based on the DraftKings fields. '''

    # urls
    API_BASE_URL = 'https://api.draftkings.com'
    DRAFTGROUPS_PATH = '/draftgroups/v1/'

    # get contests
    if league == 'PGA':
        contest = contests(sport=Sport.GOLF)
    else:
        contest = contests(sport=Sport.NFL)
    contest = contest['contests']
    contest = pd.DataFrame.from_dict(contest)
    df = contest.sort_values(by=['name'])

    dgid = df[df['name'].str.contains('PGA')]
    print(dgid.iloc[n, 8], dgid.iloc[n, 2])
    dgid = dgid.iloc[n, 2]


    # get draft table
    url = f"{API_BASE_URL}{DRAFTGROUPS_PATH}draftgroups/{dgid}/draftables"

    df = requests.get(url).json()
    df = pd.Series(df)
    df = df['draftables']
    df = pd.DataFrame(df)
    df = df.iloc[:, [0, 3, 4, 5, 6, 7, 8, 9, 10, 25]]
    df = df[df['status'] != 'O']
    df = df.drop_duplicates()
    df.rename(columns={'displayName': 'name'}, inplace=True)

    return df


def sg_data(date):

    """ Collects latest strokes gained data from db """

    # get data
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'

    # download data
    sql = ('SELECT * FROM "golf-processed"."adjusted_sg_table_processed_all"')

    data = wr.athena.read_sql_query(sql, database="golf-processed")

    # data after 2017
    data = data[data['start_date'] > date]
    data.sort_values(by=['start_date', 'trnyearid', 'full', 'round'], ascending=True, inplace=True)

    return data


def s3readcsv(bucket_name, bucket_folder, filename):

    key = f'{bucket_folder}/{filename}'
    print(key)
    client = boto3.client('s3')  # low-level functional API

    resource = boto3.resource('s3')  # high-level object-oriented API
    my_bucket = resource.Bucket(bucket_name)  # subsitute this for your s3 bucket name.
    obj = client.get_object(Bucket=bucket_name, Key=key)
    data = pd.read_csv(obj['Body'])
    return data


def writeToS3(data, bucket_name, filename, bucket_folder):
    s3 = boto3.resource('s3')
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3.Object(bucket_name, f'{bucket_folder}/{filename}').put(Body=csv_buffer.getvalue())

