#!/usr/bin/env python
# coding: utf-8
import os
import boto3
import traceback
import urllib.parse
import pandas as pd
import logging as log
import awswrangler as wr

from datetime import datetime
from typing import Iterable, Any, Tuple


log.basicConfig(level=log.INFO, format='[%(asctime)s][%(levelname)s] %(funcName)s() %(message)s', datefmt='%H:%M:%S')


def in_aws():
    """It can be used to check whether the code is running in a local environment or public cloud

    Returns:
        bool: returns True if this code is running in aws or False otherwise.
    """
    try:
        return os.name != 'nt'
    except Exception as e:
        print('in_aws() '+str(e))
        return False


def in_lambda():
    """It can be used to check whether the code is running in AWS Lambda function

    Returns:
        bool: returns True if this code is running in lambda function or False otherwise.
    """
    try:
        in_lambda = os.environ["AWS_EXECUTION_ENV"]
        if in_lambda is not None:
            return True
        else:
            return False
    except KeyError:
        return False
    except Exception as e:
        print('in_lambda() '+str(e))
        return False


def in_glue():
    """It can be used to check whether the code is running in AWS Lambda function

    Returns:
        bool: returns True if this code is running in lambda function or False otherwise.
    """
    return in_aws() and not in_lambda()


def get_env():
    """It can be used to get environment context

    Returns:
        str: returns string 'prd' for production or 'dev' to development environments.
    """
    # TODO: USE THIS CODE OR WRITE NEW
    # try:
    #     return wr.sts.get_current_identity_arn().split('gcarr-')[1].split('-')[0]
    # except IndexError:
    #     return 'prd'
    # except Exception as e:
    #     print('get_env() '+str(e))
    #     return 'prd'
    return 'dev'


def get_account_id():
    """It can be used to get amazon web services account number

    Returns:
        str: returns string with account number.
    """
    current_env = get_env()
    if current_env == 'dev':
        return '111122223333'
    elif current_env == 'prd':
        return '444555566666'
    else:
        print('ERROR !-> get_account_id() no account id for env: ', current_env)
        raise SystemExit



def boto3_assumed_role_client(resource, role_arn):
    """
    Create a boto3 client from an assumed role
    """
    sts_connection = boto3.client('sts')
    acct_b = sts_connection.assume_role(
        RoleArn=role_arn,
        RoleSessionName="cross_acct_lambda"
    )

    ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
    SECRET_KEY = acct_b['Credentials']['SecretAccessKey']
    SESSION_TOKEN = acct_b['Credentials']['SessionToken']

    client = boto3.client(
        resource,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
    )
    return client


def boto3_assumed_role_session(role_arn: str) -> boto3.Session:
    """
    Create a boto 3 session from an assumed role
    """
    sts_connection = boto3.client('sts')
    acct_b = sts_connection.assume_role(
        RoleArn=role_arn,
        RoleSessionName="cross_acct_lambda")
    ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
    SECRET_KEY = acct_b['Credentials']['SecretAccessKey']
    SESSION_TOKEN = acct_b['Credentials']['SessionToken']
    session = boto3.session.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
        region_name='us-east-1'
    )
    return session


def boto3_assumed_role_default_session(role_arn: str) -> boto3.Session:
    """
    Set default boto3 session from an assumed role
    """
    sts_connection = boto3.client('sts')
    acct_b = sts_connection.assume_role(
        RoleArn=role_arn,
        RoleSessionName="cross_acct_lambda")
    ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
    SECRET_KEY = acct_b['Credentials']['SecretAccessKey']
    SESSION_TOKEN = acct_b['Credentials']['SessionToken']
    boto3.setup_default_session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
        region_name='us-east-1'
    )


def signal_last(it:Iterable[Any]) -> Iterable[Tuple[bool, Any]]:
    iterable = iter(it)
    ret_var = next(iterable)
    for val in iterable:
        yield False, ret_var
        ret_var = val
    yield True, ret_var


def add_process_dates(df, src):
    df['data_fonte'] = src['date']
    df['data_carga'] = datetime.now()
    return df
