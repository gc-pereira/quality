#!/usr/bin/env python
# coding: utf-8
import time
import json
import boto3
import botocore
import traceback
import urllib.parse
import pandas as pd
import logging as log
import awswrangler as wr
import great_expectations as ge


from aws.utils import main as utils
from pyarrow.lib import ArrowInvalid

s3 = boto3.client('s3')


log.basicConfig(level=log.INFO, format='[%(asctime)s][%(levelname)s] %(funcName)s() %(message)s', datefmt='%H:%M:%S')


def source(event):
    source = {
            'event_time': '',
            'database': s3_key(event).split('/')[0],
            'type': s3_key(event).split('/')[1],
            'sub': s3_key(event).split('/')[1]
    }
            
    def s3_bucket(event):
        return str(urllib.parse.unquote_plus(event['Records'][0]['s3']['bucket']['name'], encoding='utf-8'))
    def s3_key(event):
        return str(urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8'))

    try:
        source['s3_bucket'] = s3_bucket(event)
        source['s3_key'] = s3_key(event)
        source['s3_path'] = s3_bucket(event)+'/'+s3_key(event)
        source['s3_uri'] = 's3://'+s3_bucket(event)+'/'+s3_key(event)
        source['s3_file'] = s3_key(event).split('/')[-1]
    except Exception as e:
        print(str(event))
        trace = traceback.format_exc()
        log.error(e)
        log.error(trace)
        raise Exception("SourceParseError")
    return source


def get_keydict(bucket_name: str, key: str) -> dict:
    response = s3.get_object(Bucket=bucket_name, Key=key)
    json_content = response['Body'].read().decode('utf-8')
    json_data = json.loads(json_content)
    return json_data


def idempotence(event_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(f"table")
    try:
        table.put_item(
            Item = {
                'event_id': event_id,
                'expires': int(time.time()) + 60 # Expire the entry in 1 minute
            },
            ConditionExpression='attribute_not_exists(event_id)'
        )
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        else:
            log.error('quality halted for repeat event_id: '+event_id)
            return False
    except Exception as e:
        log.error('problem with data-quality lock dynamodb table')
        print(str(e))
        return False


def load_data(src):
    dfs = wr.s3.read_parquet(
            src['s3_uri'],
            use_threads=True,
            chunked=100000
        )
    try:
        for df in dfs:
            test = df.dtypes # <--- random test to see if df fails
            break
        dfs = wr.s3.read_parquet(
            src['s3_uri'],
            use_threads=True,
            chunked=100000
        )
    except ArrowInvalid as e:
        log.warning(str(e))
        log.warning("fallback read with safe cast = False")
        dfs = wr.s3.read_parquet(
            src['s3_uri'],
            use_threads=True,
            chunked=100000,
            safe=False
        )
    return dfs
    

def quality(src, df, df_idx) -> tuple[pd.DataFrame, dict]:
    df = ge.from_pandas(df)
    result = {
        'chunks': df_idx,
        'row_count': len(df),
        'count_success': True,
        'count_result': -1,
        'process_file': 'None',
        'process_success': True,
        'process_result': 'None',
        'suite_file': 'None',
        'suite_success': True,
        'suite_result': 'None',
        'commit_id': 'None',
        'success': True,
    }
    
    # TODO: write quality process
    
    return df, result


def handler(event, context):
    if utils.in_aws():
        if any(utils.in_lambda(), utils.in_glue()):
            src = source(event)
            if idempotence(src['s3_uri']):
                df, result = quality(src)
                src = {**src, **result}
    else:
        return False
    