# yum install python3-devel

from pyhive import hive
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pandas as pd
import warnings
import datetime
import sys
import json

warnings.filterwarnings('ignore')
producer = KafkaProducer(bootstrap_servers='example23:6667')


df = pd.read_csv('./cpa_statuses.csv',sep=';')
df['event_dt']= df['event_dt'].astype(str).str[:-3]
df['event_dt']= pd.to_datetime(df['event_dt'], unit='s') + datetime.timedelta(hours= 3)
headers = [("__TypeId__", b"ua.kyivstar.rtn.cpa.delivery.api.model.RtnCpaDeliveryMessage")]

for x in range(0, len(df.index)):

    content= json.dumps({"mid": df['mid'][x],"data": str(df['event_dt'][x]),"status": df['status'][x]})
    future = producer.send('devops.rtn.cpa-delivery-reports',headers=headers, value=(content).encode('utf-8'))
    result = future.get(timeout=60)
    print(content)








