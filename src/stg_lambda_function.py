#!/usr/bin/env python
import snowflake.connector
import os
import boto3
from base64 import b64decode


#set all of the environment variables including the encrypted password
#variables need to be set in the aws lambda config
env_snow_account = os.environ['snowflake_account']
env_snow_user = os.environ['snowflake_user']
env_snow_pw_encryp = os.environ['snowflake_pw']
env_snow_pw_decryp = boto3.client('kms').decrypt(CiphertextBlob=b64decode(env_snow_pw_encryp))['Plaintext']

#simple query to return a value from Snowflake
query = "SELECT current_version()"


#add lambda injection point
def lambda_handler(event,context):

    #connect to snowflake data warehouse using env vars
    con = snowflake.connector.connect(
    account = env_snow_account,
    user = env_snow_user,
    password = env_snow_pw_decryp
    )

    #run the test query to check if snowflake returns a result
    cur = con.cursor()
    try:
        cur.execute(query)
        one_row = cur.fetchone()
        print(one_row[0])

    finally:
        cur.close()

    con.close()