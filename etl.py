"""
etl.py
Created on: 8/28/2019
Created by Andre Lee
Purpose: Perform ETL logic to copy data from s3 to Redshift staging tables
         and from the staging tables to the dimension and fact tables
         
Parameter(s): The code has the option to truncate all the data in the database
              or drop all the tables in the tables.
              To drop the tables - type following: run etl.py drop
              To truncate the table - type the following: run etl.py trunc
              To do nothing - type the following: run etl.py
"""
import psycopg2
import pandas as pd
import configparser
from time import time
import boto
import sys
import os
from load_fact_and_dim_tables import insert_dim_queries, insert_fact_queries, create_table_queries, copy_table_queries, truncate_tables, drop_tables


def create_tables(cur, conn, action):
    """
    Load Staging Tables function
    The function create tables by importing create_table_queries  
    from load_fact_and_dim_tables.py.  
    """ 
    
    print('action in create_tables value: ' + str(action[1]))
    
    if str(action[1]) == 'trunc':
        for query in truncate_tables:
            try:
                print(query)
                cur.execute(query)
            except Exception as e:
                print('The following query failed: ' + query)
                print (' error: ' + str(e))                
                
        print('DONE!!!')
        
    elif str(action[1]) == 'drop':
        for query in drop_tables:
            try:
                print(query)
                cur.execute(query)
            except Exception as e:
                print('The following query failed: ' + query)
                print (' error: ' + str(e))                  
                
        print('DONE!!!')
        
    else:
        print ('No Truncate or Drop table selected.')
        
    for query in create_table_queries:
        loadTimes = []
        
        print('======= Create tables =======')
        print(query)         
        t0 = time()   
        try:
            cur.execute(query)              
            conn.commit()
            loadTime = time()-t0          
            loadTimes.append(loadTime)
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
            
        except Exception as e:
            print('The following query failed: ' + query)
            print (' error: ' + str(e))
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
            conn.close()
                    
def load_staging_tables(cur, conn):
    """
    Load Staging Tables function
    The function loads log and song data json file by importing copy_table_queries  
    from load_fact_and_dim_tables.py.  The copy_table_queries uses the "copy" function to load the 
    data into the Redshift Cluster
    """    
    for query in copy_table_queries:
        loadTimes = []
        
        print('======= LOAD staging tables =======')
        print(query)         
        t0 = time() 
        try:
            cur.execute(query)              
            conn.commit()
            loadTime = time()-t0          
            loadTimes.append(loadTime)
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
                  
        except Exception as e:
            print('The following query failed: ' + query)
            print (' error: ' + str(e))
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
            conn.close()
        

def insert_dim_tables(cur, conn):
    """
    Insert dim (dimensional) Tables function
    The function insert data into the dimension tables by importing insert_table_queries  
    from load_fact_and_dim_tables.py. The dimension table loads are based on the data that was loaded into 
    the staging tables.  This mean function: load_staging_tables must run first before insert_tables.
    """      
    for query in insert_dim_queries:
        loadTimes = []

        print('======= LOAD Dimension tables =======')
        print(query)  
        t0 = time()   
        try:
            cur.execute(query)         
            conn.commit()
            loadTime = time()-t0          
            loadTimes.append(loadTime)
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
                  
        except Exception as e:
            print('The following query failed: ' + query)
            print (' error: ' + str(e))
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
            conn.close()
                  
def insert_fact_table(cur, conn):
    """
    Insert Fact Table function
    The function insert data into the fact by importing insert_table_queries  
    from load_fact_and_dim_tables.py. The fact is based on the data that was loaded into 
    the staging tables and coded ids created when the data was loaded for the dimension tables
    """      
    for query in insert_fact_queries:
        loadTimes = []

        print('======= LOAD Fact table =======')
        print(query)  
        t0 = time()   
        try:
            cur.execute(query)         
            conn.commit()
            loadTime = time()-t0          
            loadTimes.append(loadTime)
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
                  
        except Exception as e:
            print('The following query failed: ' + query)
            print (' error: ' + str(e))
            print("=== DONE IN: {0:.2f} sec\n".format(loadTime)) 
            conn.close() 

def main(argv):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()   

    action = argv
    create_tables(cur, conn, action)
    load_staging_tables(cur, conn)
    insert_dim_tables(cur, conn)
    insert_fact_table(cur, conn)
                  
    conn.close()


if __name__ == "__main__":
    main(sys.argv)
