## prcoess in parallel


from concurrent.futures import ThreadPoolExecutor
import time
import os
import sqlite3
import pandas as pd
import data_pipeline_util as util



#### populate ingestion delta tables
def execute_delta_job(ingestionTableName, stageTableName, deltaTableName, tablePrimaryKey):
    
    util.print_log("i",("Start ingestion delta process for table {}".format(ingestionTableName)))
    
    queryDB = "db_anz.db"
    
    
    ## populate I - insert dataset
    util.print_log("i", "Popualte insert data...")
    querySql = "truncate table " +  ingestionTableName + "_insert"
    queryReturn = util.query_db(queryDB, querySql)
    querySql = "create or replace table " + ingestionTableName +  "_insert as\
                select ingest.*, 'I' as record_cate\
                from " + ingestionTableName + " ingest\
                where ingest." + tablePrimaryKey + " not in (select " + tablePrimaryKey + " from " + stageTableName + ")"
    queryReturn = util.query_db(queryDB, querySql)
  

    
    ## populate U - update dataset
    util.print_log("i", "Popualte update data...")
    querySql = "truncate table " +  ingestionTableName + "_update"
    queryReturn = util.query_db(queryDB, querySql)
    querySql = "create or replace table " + ingestionTableName +  "_update as\
                select ingest.*, 'U' as record_cate\
                from " + ingestionTableName + " ingest\
                inner join " + stageTableName + " stage on stage." + tablePrimaryKey + " = ingest." + tablePrimaryKey
    queryReturn = util.query_db(queryDB, querySql)
    
   

    ## populate D - delete dataset
    util.print_log("i", "Popualte delete data...")
    querySql = "truncate table " +  ingestionTableName + "_delete"
    queryReturn = util.query_db(queryDB, querySql)
    querySql = "create or replace table " + ingestionTableName +  "_delete as\
                select ingest.*, 'I' as record_cate\
                from " + ingestionTableName + " ingest\
                inner join " + stageTableName + " stage on stage." + tablePrimaryKey + " = ingest." + tablePrimaryKey + "\
                where upper(ingest.deleted_flag) = 'Y'"
    queryReturn = util.query_db(queryDB, querySql)
    
    
    

#### load source file into delta table
def execute_ingestion_job(recordDict):
    
    util.print_log("i",("Start ingestion process for data {}".format(recordDict['sourceFileName'])))

    # time.sleep(2.5)
    ## read final source file into dataframe
    path = os.getcwd()
    fileFullPath = path+'/valid/'+recordDict['sourceFileName']
    util.print_log("i",("Loading source file {} to ingestion table {}".format(fileFullPath, recordDict['sourceTableName'])))
    
    try:
        df = pd.read_csv(fileFullPath, index_col=False, header=0)           
    
    except:
        
        ## setup notification information
        notificationSubject = 'Data ingestion for file ' + recordDict['sourceFileName']
        notificationMessage = 'Failed to ingestion file ' + recordDict['sourceFileName'] + ', source file failed to store in dataframe. Further investigation required.'
        
        ## send notification
        notificationClass = 'Failure'
        util.send_notification(notificationClass, notificationSubject, notificationMessage)
        
        util.print_log("e",("Failed to read source file to dataframe."))
        
        # return

    
    ## load ingestion data from dataframe to ingestion table in overwrite mode
    try:

        conn = sqlite3.connect(path+'\db_anz.db')
        pd.io.sql.to_sql(df, recordDict['ingestionTableName'], con=conn, if_exists='replace')
        
        conn.commit()
        conn.close()
    
    except:
        
        ## setup notification information
        notificationSubject = 'Data ingestion for file ' + recordDict['sourceFileName']
        notificationMessage = 'Failed to ingestion file ' + recordDict['sourceFileName'] + ', failed to load data from dataframe to ingestion table. Further investigation required.'
        
        ## send notification
        notificationClass = 'Failure'
        util.send_notification(notificationClass, notificationSubject, notificationMessage)
        
        util.print_log("e",("Failed to ingestion data from dataframe to ingestion table."))
        
        # return
        
    util.print_log("i", ("Complete ingestion load job for {}".format(recordDict['sourceFileName'])))
    
    print("\n------------------------------")
    
    util.print_log("i", ("Start ingestion delta job for {}".format(recordDict['sourceTableName'])))
    
    execute_delta_job(recordDict['sourceTableName'], recordDict['stageTableName'], recordDict['deltaTableName'], recordDict['tablePrimaryKey'])
    
    
    

    
#### curation process
def execute_ingestion_job(recordDict):
    
    util.print_log("i",("Start curation process for table {}".format(recordDict['curation_table_name'])))
    
    # time.sleep(3.5)
    queryDB = "db_anz.db"
    querySql = recordDict['curation_table_sql']
    
    util.print_log("i",("Execute DML: {}".format(querySql)))
    queryReturn = util.query_db(queryDB, querySql)
    return 
    
    
    
## job parallel main function
def parallel_process(recordDictList, numInParallel, parallelJob):

    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(parallelJob, recordDict) for recordDict in recordDictList]
