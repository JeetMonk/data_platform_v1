## raw source file existence check

import os
import glob
import data_pipeline_util as util

def source_file_existence_check(recordDict):
    
    print("\n----------------------------------------")
    print("Start checking existance of raw source file: {} ...".format(recordDict['sourceFileName']))
    
    path = os.getcwd()
    fileExistenceFlag = 0
    
    existFileList = glob.glob(path+'/'+recordDict['sourceFilePath']+'/'+recordDict['sourceFileName']+'*')
    NoFileFound = len(existFileList)
    util.print_log("i", ("Existing file(s): {}".format(existFileList)))
    util.print_log("i", ("Expecting {} file(s) and {} file(s) found".format(recordDict['sourceFilePartitionNumber'], NoFileFound)))
    
    if recordDict['sourceFilePartitionNumber'] == len(existFileList):
        fileExistenceFlag = 0
        ## setup notification information
        notificationSubject = 'File Existence Check for File ' + recordDict['sourceFileName']
        notificationMessage = 'File existence check completed for file ' + recordDict['sourceFileName']
        
        util.print_log("i", "Pass file existence check")
        
        
    if recordDict['sourceFilePartitionNumber'] > len(existFileList) and recordDict['sourceFileMandatoryFlag'] == 'Y':
        fileExistenceFlag = 1
        ## setup notification information
        notificationSubject = 'File Existence Check for Mandatory File ' + recordDict['sourceFileName']
        notificationMessage = 'Missing mandatory file(s) found for ' + recordDict['sourceFileName'] + ', expecting ' + str(recordDict['sourceFilePartitionNumber']) + ' found ' + str(NoFileFound)
        util.print_log("e", "Not enough mandatory file(s) found ! Send failure notification")
        return
        

    if recordDict['sourceFilePartitionNumber'] > len(existFileList) and recordDict['sourceFileMandatoryFlag'] == 'N':
        fileExistenceFlag = 2
        ## setup notification information
        notificationSubject = 'File Existence Check for Optional File ' + recordDict['sourceFileName']
        notificationMessage = 'Missing optional file(s) found for ' + recordDict['sourceFileName'] + ', expecting ' + str(recordDict['sourceFilePartitionNumber']) + ' found ' + str(NoFileFound)
        util.print_log("w","Not enough optional file(s) found ! Send warning notification")
        return
        
    ## send notification
    notificationClass = 'Success' if fileExistenceFlag == 0 else 'Failure' if fileExistenceFlag == 1 else 'Warning'
    util.send_notification(notificationClass, notificationSubject, notificationMessage)
    
    
    print("i", ("Complete raw source file {} existence check.".format(recordDict['sourceFileName'])))
    
    return