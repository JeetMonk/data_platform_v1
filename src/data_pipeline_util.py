## print log 
from datetime import datetime

def print_log(logClass, logMessage):
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%Y-%m-%d %H:%M:%S")
    
    logLevel = "INFO" if logClass == 'i' else 'WARNING' if logClass == 'w' else 'ERROR' if logClass == 'e' else 'FATAL' if logClass == 'f' else 'UNKNOWN'
    
    logInfo = '[' + timestampStr + '] - ' + logLevel + ' - ' + logMessage
    
    print(logInfo)




## query DB, can execute any SQL
import sqlite3
import os

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def query_db(dbName, sqlDetail):
    path = os.getcwd()
    try:
        conn = sqlite3.connect(path+'\\'+ dbName)
        conn.row_factory = dict_factory
        cur = conn.cursor()
        
        cur.execute(sqlDetail)
        recordRows = cur.fetchall()
        conn.close()
        return recordRows
    except:
        print_log("f", "Failed to query DB")




## send notification
import boto3
from datetime import datetime

def send_notification(notificationClass, notificationSubject, notificationMessage):
    
    print("\n----------------------------------------")
    nowTime = datetime.now()
    # nowTime = datetime.datetime.now()
    
    print_log("i", ("notificationClass: {}".format(notificationClass)))
    print_log("i", ("notificationSubject: {}".format(notificationSubject)))
    print_log("i", ("notificationMessage: {}".format(notificationMessage)))
    
    snsSubject = notificationClass + ' - ' + notificationSubject
    print(snsSubject)
    
    snsMessage = notificationMessage + ' at ' + str(nowTime)
    print_log("i",snsMessage)
    
    sns_client = boto3.client("sns", region_name="ap-southeast-2")
    
    
    ## setup your credentials and SNS ARN to run below code
    topicArn = "arn:aws:sns:ap-southeast-2:127693******:data-pipelines-notifications-san-ap-southeast-2"
    try:    
        sns_client.publish(TopicArn=topicArn, Message=snsMessage, Subject=snsSubject)
    except:
        print_log("w", "setup your credentials and SNS ARN to call SNS service.")
