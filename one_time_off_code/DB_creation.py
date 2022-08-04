## create DB

import os
import sqlite3


## create a db

path = os.getcwd()
print(path)

conn = sqlite3.connect(path+'\db_anz.db') 

conn.close()