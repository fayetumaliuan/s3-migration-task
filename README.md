# s3-migration-task
S3 migration task from one bucket to another


Steps taken:

1. Create git repository and clone
git clone https://github.com/fayetumaliuan/s3-migration-task.git

2. Minio setup

Installed Minio
brew install minio/stable/minio

export MINIO_CONFIG_ENV_FILE=/etc/default/minio
minio server '/Users/fayebeatriz/Documents/Faye Docs/Resume/Sketch' --console-address :9090

3. Postgres setup

Installed postgres: https://postgresapp.com

Run schema.sql

CREATE ROLE migrateuserrw WITH
  LOGIN
  NOSUPERUSER
  INHERIT
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION
  ENCRYPTED PASSWORD 'SCRAM-SHA-256$4096:hJeoNH1/eAK8QBaBJXsmQw==$pNXJr3H0oZtOaPU1nXtQY2blQhG+yRcjpGzQFG8WWJA=:XmaqBixfpfeplhZYSIlJSY4EvE1KpT/vRP3o4RQey2s=';
GRANT CONNECT ON DATABASE proddatabase TO migrateuserrw;
GRANT SELECT, INSERT, UPDATE ON avatars TO migrateuserrw;
GRANT USAGE, SELECT ON SEQUENCE avatars_id_seq TO migrateuserrw;

4. Seeder file setup

Run seeder.py 

---------------------------------------------------------------
After this step, the environment and configuration is complete.
---------------------------------------------------------------

5. Test migration tool

How to run migration-tool.py

This python script lists all objects in a legacy bucket, copies object to the new bucket, updates the database for the new path, and deletes the object from the old path. 

In case the job fails in any of these steps, the python file can be ran again since the old path/object is only deleted once this whole process is completed. If the job fails, all the remaining objects in the old path can be re-ran until all objects are moved and deleted from the old path. 

a. Change applicable variables in lines 14-24
b. Run python file as is, without paramaters


