## raw source file validation

import os
import pandas as pd
import data_pipeline_util as util


def source_file_validation(recordDict):
    print("\n----------------------------------------")
    print("Start validation check for raw source file: {} ...".format(recordDict['sourceFileName']))
    
    path = os.getcwd()
    fileFullPath = path+'/'+recordDict['sourceFilePath']+'/'+recordDict['sourceFileName']
    
    
    ## has header
    if len(recordDict['sourceFileHeader']) > 0:
        util.print_log("i", ("The source file has a header: {}".format(recordDict['sourceFileHeader'])))

        ## header validation
        try:
            df = pd.read_csv(fileFullPath, index_col=False, header=0)           
        except:
            
            ## setup notification information
            notificationSubject = 'File Validation Check for File ' + recordDict['sourceFileName']
            notificationMessage = 'Failed to validate file ' + recordDict['sourceFileName'] + ', due to unable to read the file. Further investigation required.'
            
            util.print_log("e", "Failed to read the source file.")
            
            ## send notification
            notificationClass = 'Failure'
            util.send_notification(notificationClass, notificationSubject, notificationMessage)
            return
        
        
        ## header not match
        if list(df.columns.values) != recordDict['sourceFileHeader']:

            ## setup notification information
            notificationSubject = 'File Validation Check for File ' + recordDict['sourceFileName']
            notificationMessage = 'Failed to validate file ' + recordDict['sourceFileName'] + ', due to expectied header ' + recordDict['sourceFileHeader'] + ' found ' + list(df.columns.values)
            
            ## send notification
            notificationClass = 'Failure'
            util.send_notification(notificationClass, notificationSubject, notificationMessage)
            
            util.print_log("e", ("Expected header {}, found header {}".format(recordDict['sourceFileHeader'], list(df.columns.values))))
            return
        
        else:
            util.print_log("i", "Header validation passed.")
            
        
    ## no header
    else:
        
        try:
            df = pd.read_csv(fileFullPath, index_col=False, header=None)
            util.print_log("i","Source file has fix structure and has been read into dataframe")
        except:
            
            ## setup notification information
            notificationSubject = 'File Validation Check for File ' + recordDict['sourceFileName']
            notificationMessage = 'Failed to validate file ' + recordDict['sourceFileName'] + ', due to unable to read the file. Further investigation required.'
            util.print_log("e", "Failed to read the source file.")
            
            ## send notification
            notificationClass = 'Failure'
            util.send_notification(notificationClass, notificationSubject, notificationMessage)
            return


            
    ## column number validation
    if len(df.columns) != recordDict['NumberofColumn']:

        ## setup notification information
        notificationSubject = 'File Validation Check for File ' + recordDict['sourceFileName']
        notificationMessage = 'Failed to validate file ' + recordDict['sourceFileName'] + ', due to expectied number of column ' + str(recordDict['NumberofColumn']) + ', but found ' + str(len(df.columns))
        util.print_log("e", ("Expected number of column {}, but found header {}".format(recordDict['NumberofColumn'], len(df.columns))))

        ## send notification
        notificationClass = 'Failure'
        util.send_notification(notificationClass, notificationSubject, notificationMessage)
        return
        
    else:
        util.print_log("i", "Column number validation passed.")
        
    
    
    ## primaryKey validation
    ## get the primary key(s) from the control table i.e. recordDict['primaryKey'] and count if the value(s) of that column(s) > 1
    ## put the bad records into a exception file
    
    
    ## Number Type Columns validation
    ## get the Number Type Columns  from the control table i.e. recordDict['NumberTypeColumns'], check if any row contains non-numeric values
    ## put the bad records into a exception file
    
    ## ... and other validations ... could be more based on business requirement.
    ## ......

    
    util.print_log("i", ("Complete validation for file {}.".format(recordDict['sourceFileName'])))
    
    return