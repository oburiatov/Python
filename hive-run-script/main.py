from pyhive import hive
import pandas as pd
import warnings
import os

warnings.filterwarnings('ignore')

hivehost=os.getenv('HIVE_HOSTNAME')
hiveport=os.getenv('HIVE_PORT')
hiveusername=os.getenv('HIVE_USERNAME')
hivepassword=os.getenv('HIVE_PASSWORD')

periodFrom=os.getenv('PERIOD_FROM')
periodTo=os.getenv('PERIOD_TO')

hiveconn = hive.Connection(host=hivehost, port=10000, username=hiveusername, auth='LDAP', password=hivepassword)
cur = hiveconn.cursor()

query = '''insert into agg_dpi.agg_hdr_id_monthly partition (hmonth='%s')
select 
	id,
	imsi,
	imei,
	service_id,
	lvl5_protocol,
	host,
	sub_domain_name,
	domain_name,
	line_id,
	pipe_id,
	vc_id,
	sum(vol_in_amt) as vol_in_amt,
	sum(vol_out_amt) as vol_out_amt,
	sum(duration_msec_amt) as duration_msec_amt,
	sum(hour_cnt) as hour_cnt,
	sum(session_cnt) as session_cnt,
	sum(total_cnt) as total_cnt,
	min(min_server_init_resp_time_msec_amt) as min_server_init_resp_time_msec_amt,
	max(max_server_init_resp_time_msec_amt) as max_server_init_resp_time_msec_amt,
	min(first_activity_dt) as first_activity_dt,
	max(last_activity_dt) as last_activity_dt
from agg_dpi.agg_hdr_daily
where hday >= '%s'  and hday < '%s' 
group by 	id,
	imsi,
	imei,
	service_id,
	lvl5_protocol,
	host,
	sub_domain_name,
	domain_name,
	line_id,
	pipe_id,
	vc_id''' % (periodFrom, periodFrom, periodTo)

cur.execute(query)

print ("Query \n" + query + "\n successfully completed!")
