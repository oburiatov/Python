from pyhive import hive
import pandas as pd
import warnings
import datetime
import sys
warnings.filterwarnings('ignore')


hiveconn = hive.Connection(host='example08', port=10000, username='username', auth='LDAP', password='passwd')

df_databases = pd.read_sql("show databases ",
    hiveconn)

for i in range(len(df_databases)):
    database = (df_databases.loc[i, "database_name"])
    df_tables = pd.read_sql("show tables in " + database,
    hiveconn)

    for j in range(len(df_tables)-4):
        table = (df_tables.loc[j+4, "tab_name"])
        df_table_description = pd.read_sql("describe formatted  " +database + "." + table,
            hiveconn)
        comment = False
        file_object = open('results.csv', 'a')
        for k in range(len(df_table_description)):
            if str(df_table_description.loc[k, "data_type"]).strip() == str("comment"):
                result = database + "." + table +", TABLE, " + df_table_description.loc[k, "comment"]
                file_object.write(result +"\n")
                file_object.close()
                comment = True
        if comment == False:
            result = database + "." + table +", TABLE, NO DESCRIPTION" 
            file_object.write(result+"\n")
            file_object.close()





