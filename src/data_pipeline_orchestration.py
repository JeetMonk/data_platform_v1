
import data_pipeline_util as util
import data_file_existence_check as fec
import data_file_validation as valid
import data_file_consolidation as conso
import data_parallel_processes as paral




## data ingestion in parallel
def source_file_ingestion(recordDictList):
    
    print("\n----------------------------------------")
    
    ## setup max number of parallel process, this nunmber could be stored in a config file.
    numInParallel = 5
    parallelJob = 'execute_ingestion_job'
    
    res = paral.parallel_process(recordDictList, numInParallel, parallelJob)
    resultList = [i.result(timeout=3600) for i in res] # This is a blocking call.




## data modeling /curation layer in parallel
def curation_layer_process(recordDictList):
        
    print("\n----------------------------------------")
    
    ## setup max number of parallel process, this nunmber could be stored in a config file.
    numInParallel = 5
    parallelJob = 'execute_curation_job'
    
    res = paral.parallel_process(recordDictList, numInParallel, parallelJob)
    resultList = [i.result(timeout=3600) for i in res]




## data provisioning layer in parallel
#### this could be implemented like curation layer i.e. a control table to manages all the processes.




## orchestration
if __name__ == '__main__':


    #### source file existence check
    print("\n------------------------------------------------------------")
    util.print_log("i", "File Existence Check Process Starts...")
    queryDB = "db_anz.db"
    querySql = "select * from control_source_file_details where sourceFileDeletedFlage != 'Y'"
    recordRowsList = util.query_db(queryDB, querySql)
    print(recordRowsList)
    for recordDict in recordRowsList:
        # print("Data record: {}".format(recordDict))
        fec.source_file_existence_check(recordDict)
    util.print_log("i", "File Existence Check Process Completed...")




    #### source file validation
    print("\n------------------------------------------------------------")
    util.print_log("i", "File Validation Check Process Starts...")
    queryDB = "db_anz.db"
    querySql = "select * from control_source_file_data_validation"
    recordRowsList = util.query_db(queryDB, querySql)
    print(recordRowsList)
    for recordDict in recordRowsList:
        valid.source_file_validation(recordDict)
    util.print_log("i", "File Validation Completed...")




    ## data file consolidation
    print("\n------------------------------------------------------------")
    util.print_log("i", "File Consolidation Process Starts...")
    queryDB = "db_anz.db"
    querySql = "\
        select\
            file.sourceFileName,\
            file.sourceFilePartitionNumber,\
            data.sourceFileHeader,\
            data.columnDelimiter\
        from control_source_file_details file\
        inner join control_source_file_data_validation data on data.sourceFileID = file.sourceFileID\
        where file.sourceFilePartitionNumber > 1 and file.sourceFileDeletedFlage != 'Y'"
    recordRowsList = util.query_db(queryDB, querySql)
    print(recordRowsList)
    for recordDict in recordRowsList:
        conso.source_file_consolidation(recordDict)
    util.print_log("i", "File Consolidation Completed...")




    ## data file ingestion
    print("\n------------------------------------------------------------")
    util.print_log("i", "File Ingestion Process Starts...")
    queryDB = "db_anz.db"
    querySql = "select * from control_source_file_delta_table"
    recordRowsList = util.query_db(queryDB, querySql)
    print(recordRowsList)
    source_file_ingestion(recordRowsList)
    util.print_log("i", "File Validation Completed...")




    ## data file curation
    print("\n------------------------------------------------------------")
    util.print_log("i", "Curation Layer Process Starts...")
    queryDB = "db_anz.db"
    querySql = "select * from control_curation_stream"
    recordRowsList = util.query_db(queryDB, querySql)
    print(recordRowsList)
    curation_layer_process(recordRowsList)
    # print_log("i", "File Validation Completed...")