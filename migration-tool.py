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
import concurrent.futures
import csv


# Env variables for script configuration
DB_CONN_STRING = os.getenv('DB_CONN_STRING', 'postgres://migrateuserrw:migrate101@127.0.0.1/proddatabase')

# S3 bucket names to use
S3_BUCKET_NAME_LEGACY = os.getenv('S3_BUCKET_NAME', 'legacy-s3')
S3_BUCKET_NAME_PROD = os.getenv('S3_BUCKET_NAME_PROD', 'production-s3')

# S3 connection details
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'http://localhost:9000')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

allobjects_fname = 'allobjects.csv'
completedobjects_fname = 'completedobjects.csv'

def create_object_list(s3, legacybucket):
    try:
        bucket = s3.Bucket(legacybucket)
        allobjects = pd.DataFrame()
        #lists all objects in the legacy bucket
        for obj in bucket.objects.all(): 
            allobjects = pd.concat([allobjects,pd.DataFrame([obj.key])])
        
        #all objects are logged into a csv file      
        allobjects.to_csv(allobjects_fname, sep=',', na_rep='', header=False, index=False)
    except Exception as e:
        logging.error(f"Error while listing files: {e}")
        sys.exit(1)
    
def update_db_row(path,newpath):
#updates the path of the current object being copied to the new path
    try:
        cur = conn.cursor()
        cur.execute("UPDATE avatars SET path = %s WHERE path = %s", [newpath,path])
        conn.commit()
    except Exception as e:
        logging.error(f"Error updating the database: {e}")
        sys.exit(1)

def copy_s3_objects(conn, s3, legacybucket, prodbucket,obj):
    try:
        #print('Now copying: ', obj)
        bucket = s3.Bucket(legacybucket)
        
        s3_conn = boto3.client('s3',endpoint_url=S3_ENDPOINT_URL,aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_DEFAULT_REGION)
        
        source=legacybucket+'/'+obj
        newpath='avatar/'+re.sub('image/*', '', obj)

        #copies object to the new bucket
        s3_conn.copy_object(Bucket=prodbucket,CopySource=f"{source}", Key=f"{newpath}")

        #updates the database for the new path
        update_db_row(obj,newpath)

        #deletes the object from the old path
        bucket.Object(obj).delete()
        return pd.DataFrame([obj])    
    
    except Exception as e:
        logging.error(f"Error while moving files: {e}")
        sys.exit(1)
    
if __name__ == "__main__":
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
        
    except Exception as e:
        logging.error(f"Error while connecting to S3: {e}")
        sys.exit(1)

    
    create_object_list(s3_resource, S3_BUCKET_NAME_LEGACY)
    file = open(allobjects_fname, "r")
    files = list(csv.reader(file, delimiter=","))
    file.close()  
    completedobjects = pd.DataFrame()
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(copy_s3_objects,conn,s3_resource, S3_BUCKET_NAME_LEGACY, S3_BUCKET_NAME_PROD,file[0]): file for file in files}
        for future in concurrent.futures.as_completed(future_to_file):
            url = future_to_file[future]
            try:
                completeddata = future.result()
                completedobjects = pd.concat([completedobjects,completeddata])
            except Exception as e:
                logging.error(f"Error while connecting to S3: {e}")
                sys.exit(1)
    
    
    completedobjects.to_csv(completedobjects_fname, sep=',', na_rep='', header=False, index=False)
    
    # get the end time
    et = time.time()

    # get the execution time
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')