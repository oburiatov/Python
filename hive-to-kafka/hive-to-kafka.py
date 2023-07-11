# yum install python3-devel
from kafka import KafkaConsumer
from kafka import KafkaProducer
from pyhive import hive
import pandas as pd


# consumer = KafkaConsumer(bootstrap_servers='example23:9888')
producer = KafkaProducer(bootstrap_servers='example23:9888')


from pyhive import hive
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


hiveconn = hive.Connection(host='example08', port=10000, username='uname', database='sandbox', auth='LDAP', password='passwd')
cursor=hiveconn.cursor()

df = pd.read_sql("select id, \
                    144 as scor_formula_key,\
                    round(max(double(score)), 5) as score,\
                    substr(CURRENT_TIMESTAMP(), 1, 19) as dwh_ins_dt\
                FROM sandbox.ml_sb_nonks_score_new_m where 1=1 and score_date = '2022-08-01' and score < 1 and score > 0.00001 \
                GROUP BY id",
    hiveconn)

df.rename(columns={ df.columns[0]: "id" }, inplace = True)
df.rename(columns={ df.columns[1]: "scor_formula_key" }, inplace = True)
df.rename(columns={ df.columns[2]: "score" }, inplace = True)
df.rename(columns={ df.columns[3]: "dwh_ins_dt" }, inplace = True)

for x in range(0, len(df.index)):
    content=str(df['id'].iloc[x])+"|"+str(df['scor_formula_key'].iloc[x])+"|"+str(df['score'].iloc[x])+"|"+str(df['dwh_ins_dt'].iloc[x])
    kafka_key= str(df['id'].iloc[x])+"|"+str(df['scor_formula_key'].iloc[x])
    future = producer.send('numtest', key=kafka_key.encode('utf-8'),value=(content).encode('utf-8'),partition=3)
    result = future.get(timeout=60)






