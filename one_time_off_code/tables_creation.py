import os
import sqlite3


## create control table for source files existence check

path = os.getcwd()

conn = sqlite3.connect(path+'\db_anz.db') 
cur = conn.cursor()

cur.execute('\
    create table if not exists control_source_file_details (\
        sourceFileID integer primary key,\
        sourceFileName text not null,\
        sourceFilePath text not null,\
        sourceFilePartitionNumber integer not null,\
        sourceFileMandatoryFlag text not null,\
        sourceFileDeletedFlage text,\
        recordCreateDate text not null,\
        recordUpdatedDate text,\
        recordDeletedDate text\
    )') 

conn.commit()
cur.close()
conn.close()





## insert 2 sample data to control_source_file_existence

path = os.getcwd()

conn = sqlite3.connect(path+'\db_anz.db') 
cur = conn.cursor()

cur.execute('\
    insert into control_source_file_details(sourceFileID, sourceFileName, sourceFilePath, sourceFilePartitionNumber, sourceFileMandatoryFlag, recordCreateDate, recordUpdatedDate, sourceFileDeletedFlage, recordDeletedDate)\
    values (1, "transaction_historical", "landing", 2, "Y", "20220803215010", "", "", "")')
cur.execute('\
    insert into control_source_file_details(sourceFileID, sourceFileName, sourceFilePath, sourceFilePartitionNumber, sourceFileMandatoryFlag, recordCreateDate, recordUpdatedDate, sourceFileDeletedFlage, recordDeletedDate)\
    values (2, "merchant_details", "landing",1, "N", "20220803215010", "", "", "")')

conn.commit()
cur.close()
conn.close()




## create control table for source files validation check

path = os.getcwd()

conn = sqlite3.connect(path+'\db_anz.db') 
cur = conn.cursor()

cur.execute('\
    create table if not exists control_source_file_data_validation (\
        sourceFileID integer primary key,\
        sourceFileName text not null,\
        sourceFilePath text not null,\
        sourceFileHeader text,\
        sourceFileFooter text,\
        columnDelimiter text not null,\
        columnEnclosure text,\
        primaryKey text,\
        NumberofColumn integer not null,\
        NumberTypeColumns text,\
        DateTimeTypeColumns text,\
        BooleanTypeColumns text,\
        mandatoryColumns text\
    )') 

conn.commit()
cur.close()
conn.close()




## create control table for source data ingestion process

path = os.getcwd()

conn = sqlite3.connect(path+'\db_anz.db') 
cur = conn.cursor()

cur.execute('\
    create table if not exists control_source_file_delta_table (\
        sourceFileID integer not null,\
        sourceFileName text not null,\
        sourceFileHeader text,\
        tablePrimaryKey text not null,\
        ingestionTableID integer not null,\
        ingestionTableName text not null,\
        deltaTableID integer not null,\
        deltaTableName text not null,\
        stageTableID integer not null,\
        stageTableName text not null\
    )') 

conn.commit()
cur.close()
conn.close()




## create ingestion table ingestion_transaction_historical

path = os.getcwd()

conn = sqlite3.connect(path+'\db_anz.db') 
cur = conn.cursor()

cur.execute('\
    create table if not exists ingestion_transaction_historical (\
        transaction_id integer not null,\
        transaction_amount integer not null,\
        transaction_date text not null,\
        transaction_approved text not null,\
        transcation_status text not null,\
        payer_merchant_id integer not null,\
        payee_merchant_id integer not null,\
        created_date text not null,\
        updated_date text,\
        deleted_flag text,\
        deleted_date text\
    )') 

conn.commit()
cur.close()
conn.close()




## create ingestion table ingestion_merchant_details

path = os.getcwd()

conn = sqlite3.connect(path+'\db_anz.db') 
cur = conn.cursor()

cur.execute('\
    create table if not exists ingestion_merchant_details (\
        merchant_id integer not null,\
        merchant_company_name text,\
        merchant_first_name text,\
        merchant_last_name text,\
        merchant_address1 text,\
        merchant_address2 text,\
        merchant_suburb text,\
        merchant_state text,\
        merchant_postcode integer,\
        merchant_email text,\
        merchant_contact_number text\
    )') 

conn.commit()
cur.close()
conn.close()




## create control table for curation management

path = os.getcwd()

conn = sqlite3.connect(path+'\db_anz.db') 

cur = conn.cursor()

cur.execute('\
    create table if not exists control_curation_stream (\
        curation_table_name text not null,\
        curation_table_type text not null,\
        curation_table_sql text not null,\
        active_flag text not null\
    )') 

conn.commit()
cur.close()
conn.close()