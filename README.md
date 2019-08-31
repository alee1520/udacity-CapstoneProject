Purpose: 
To compare crime data between NYC and LA.  Major attribute to compare are gender, premise , location, and race between suspect and victim.
The data can be use to create a dashboard to show trend of crime in a 10 year span either in LA or NYC or both.

The following technologies will be used:
	1. AWS cloud s3 bucket
	2. Redshift
	3. Python 
	4. Airflow 

Scope: 
To read data files from s3 bucket to Amazon Redshift staging tables.  
When the data loaded to the staging tables will load to dimension tables and to the fact table.

Addressing additional scenarios:

The data was increased by 100x.
	Redshift is AWS flagship datawarehouse that has the capacity to handle petabytes of data at scale.  In order to scale up to scale as needed, additional nodes can be added to the existence Redshift instance.
	
The pipelines would be run on a daily basis by 7 am every day.
	In order to have the datapipes/ETL process to run daily@7am, Airflow dags would need to be created.  The airflow dag schedule can be set to run to pick up the previous day plus the current data so it can effectively pull in and have the most current data.
	
The database needed to be accessed by 100+ people.	
	Redshift is built to handle BI report which will allow multiple users to connect and access the database for reporting.  A connection limit can be set to allow additional users to connect to the database.
	
Instructions:
Load the following data files to a s3 bucket from /dataFiles.

    NYPD_Complaint_Data_Historic.csv
    LAX_Crime_Data_from_2010_to_Present.csv
    us-zip-code-latitude-and-longitude.csv
    
Create an AWS Redshift instance with 4 to 8 nodes.

    instance name = my-redshift-(region name)
    database name = crimedb
    username= awsuser
    
Modify dwf.cfg to have the following information

    [CLUSTER]
    HOST=<redshift endpoint>
    DB_NAME='crimedb'
    DB_USER='awsuser'
    DB_PASSWORD=
    DB_PORT=5439

    [IAM_ROLE]
    ARN=<ARN>

    [AWS]
    AWS_ACCESS_KEY_ID=<your aws access key id >
    AWS_SECRET_ACCESS_KEY=<your secret access key>

    [S3]
    LA_DATA_FILE  ='s3://<bucket name>/LAX_Crime_Data_from_2010_to_Present.csv'
    NYC_DATA_FILE ='s3://<bucket name>/NYPD_Complaint_Data_Historic.csv'
    US_CITIES_FILE = 's3://<bucket name>/us-zip-code-latitude-and-longitude.csv'

Before running the snippet of code(s) in the Jupyter cell.  Run the following code the terminal: pip install s3fs
The individual cell back be executed within "Capstone Project.ipynb".  The source data files can be viewed so in the notebook.

To run the full ETL process open notebook "execute_full_etl_script_and_datacheck_and_reports.ipynb"
The notebook has the following command entered "run etl.py drop"
	Note:  etl.py script has a parameter option to "drop" or "trunc"
		"drop" option will drop the existing tables and recreate them
		"trunc" option will clean/delete out all the before the etl process begins.
		
"execute_full_etl_script_and_datacheck_and_reports.ipynb" also has data quality checks and sample report(s).

Use of the data.
There are multiple uses the data can be applied.  The crime data for NYC and LA can be used to track trends in crimes between the 2 cites or per city.  Track crime by day, month, year by gender and race and type of crime.  Additional outside source data can be applied to enrich the existing data.
