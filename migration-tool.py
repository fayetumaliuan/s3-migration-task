#!/usr/bin/env python

import sys
import os
import logging
import argparse
import psycopg2
import boto3
import pandas as pd
import re
import time

# Env variables for script configuration
DB_CONN_STRING = os.getenv('DB_CONN_STRING', 'postgres://migrateuserrw:migrate101@127.0.0.1/proddatabase')

# S3 bucket name to use. It should exist and be accessible to your AWS credentials
S3_BUCKET_NAME_LEGACY = os.getenv('S3_BUCKET_NAME', 'legacy-s3')
S3_BUCKET_NAME_PROD = os.getenv('S3_BUCKET_NAME_PROD', 'production-s3')

# S3 connection details
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'http://localhost:9000')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

def update_db_row(path,newpath):
#updates the path of the current object being copied to the new path
    try:
        cur = conn.cursor()
        cur.execute("UPDATE avatars SET path = %s WHERE path = %s", [newpath,path])
        conn.commit()
    except Exception as e:
        logging.error(f"Error updating the database: {e}")
        sys.exit(1)

def copy_s3_objects(conn, s3,s3_conn, legacybucket, prodbucket):

    try:
        bucket = s3.Bucket(legacybucket)
        completedobjects = pd.DataFrame()
        #lists all objects in the legacy bucket
        for obj in bucket.objects.all(): 
            source=legacybucket+'/'+obj.key
            newpath='avatar/'+re.sub('image/*', '', obj.key)
            
            #copies object to the new bucket
            s3_conn.copy_object(Bucket=prodbucket,CopySource=f"{source}", Key=f"{newpath}")
            
            #updates the database for the new path
            update_db_row(obj.key,newpath)
            completedobjects = pd.concat([completedobjects,pd.DataFrame([obj.key])])
            
            #deletes the object from the old path
            bucket.Object(obj.key).delete()
        
        #completed objects are logged into a csv file      
        completedobjects.to_csv('completedobjects.csv', sep=',', na_rep='', header=False, index=False)
        print("All objects in legacy folder moved in production folder. For a list of files processed, please refer to objects_to_move.csv file")
    except Exception as e:
        completedobjects.to_csv('completedobjects.csv', sep=',', na_rep='', header=False, index=False)
        logging.error(f"Error while moving files: {e}")
        sys.exit(1)
    
if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description='This script seeds the database and s3 bucket with the number of legacy avatars passed as a first argument.')
    #parser.add_argument('number_of_avatars', type=int, help='Number of legacy avatars to create')
    #args = parser.parse_args()
    # get the start time
    st = time.time()

    # Connect to db
    try:
        conn = psycopg2.connect(DB_CONN_STRING)
    except Exception as e:
        logging.error(f"Error while connecting to the database: {e}")
        sys.exit(1)

    # Initialize s3 resource
    try:
        
        s3_resource = boto3.resource("s3",endpoint_url=S3_ENDPOINT_URL,aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_DEFAULT_REGION)
        s3client = boto3.client('s3',endpoint_url=S3_ENDPOINT_URL,aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_DEFAULT_REGION)
    except Exception as e:
        logging.error(f"Error while connecting to S3: {e}")
        sys.exit(1)
    
    copy_s3_objects(conn,s3_resource,s3client, S3_BUCKET_NAME_LEGACY, S3_BUCKET_NAME_PROD)
    
    # get the end time
    et = time.time()

    # get the execution time
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')