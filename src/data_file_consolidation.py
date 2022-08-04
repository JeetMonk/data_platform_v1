## data file consolidation
####  put all file together

import os
import glob
import pandas as pd
import data_pipeline_util as util

def source_file_consolidation(recordDict):

    print("\n----------------------------------------")
    util.print_log("i", ("Start consolidation for source file: {} ...".format(recordDict['sourceFileName'])))
    
    path = os.getcwd()
    sourceFileList = glob.glob(path + '/valid/'+recordDict['sourceFileName']+'*')
    targetFileFullPath = path + '/valid/'+recordDict['sourceFileName']+'_final.csv'
    
    try:                
        ## has header on each of partitioned files
        if len(recordDict['sourceFileHeader']) > 0:

            df = pd.concat((pd.read_csv(fileName, header=0) for fileName in sourceFileList))
            df.to_csv(targetFileFullPath, index=False)


        ## no header on each of partitioned files, but populate header for final target file
        else:

            df = pd.concat((pd.read_csv(fileName, header=None) for fileName in sourceFileList))

            fileHeader = recordDict['sourceFileHeader']
            df.to_csv(targetFileFullPath, header=fileHeader, index=False)
        
    except:
        
        ## setup notification information
        notificationSubject = 'File Consolidation for file ' + recordDict['sourceFileName']
        notificationMessage = 'Failed to consolidate file ' + recordDict['sourceFileName'] + ', Further investigation required.'
        
        ## send notification
        notificationClass = 'Failure'
        util.send_notification(notificationClass, notificationSubject, notificationMessage)
        util.print_log("e", ("Failed to onsolidate file {}".format(recordDict['sourceFileName'])))
        return
    
    util.print_log("i", ("Complete consolidation for file {}.".format(recordDict['sourceFileName'])))
    
    return