# yum install python3-devel

from pyhive import hive
import pandas as pd
from pyhive import hive
import pandas as pd
import warnings
import datetime
import sys

warnings.filterwarnings('ignore')
orig_stdout = sys.stdout
f = open('out.txt', 'w')
sys.stdout = f


hiveconn = hive.Connection(host='example08', port=10000, username='', database='ods_rtn', auth='LDAP', password='')
cur = hiveconn.cursor()

df = pd.read_csv('./cpa_statuses.csv',sep=';')
df['event_dt']= df['event_dt'].astype(str).str[:-3]
df['event_dt']= pd.to_datetime(df['event_dt'], unit='s') + datetime.timedelta(hours= 3)



for x in range(0, len(df.index)):
    
    print("INSERT INTO  ods_rtn.cpa_delivery_reports partition(hday='"+str(df['event_dt'][x]).split(' ', 1)[0]+"') VALUES ('"+df['mid'][x]+"', '"+ str(df['event_dt'][x])+"', '"+df['status'][x]+"')")
    cur.execute(query)


sys.stdout = orig_stdout
f.close()





