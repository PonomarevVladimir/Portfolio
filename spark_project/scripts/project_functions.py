from datetime import datetime
import pyspark
import pyspark.sql.functions as f
from datetime import datetime
import datetime as dtm
import math as m

def dist(lat1, lon1, lat2, lon2):
    dLat = (lat2 - lat1) * m.pi / 180.0
    dLon = (lon2 - lon1) * m.pi / 180.0
 
    lat1 = (lat1) * m.pi / 180.0
    lat2 = (lat2) * m.pi / 180.0
 
    a = m.pow(m.sin(dLat / 2), 2) + m.pow(m.sin(dLon / 2), 2) * m.cos(lat1) * m.cos(lat2)
    rad = 6371.0
    c = 2 * m.asin(m.sqrt(a))
    return rad * c

def input_paths(path, date, depth):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return [f"{path}/events/date={(dt-dtm.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def add_local_time(df_in):
    df = df_in.withColumn('timezone', f.lit('Australia/Sydney'))
    
    aest_list = ['Brisbane', 'Gold Coast', 'Townsville', 'Ipswich', 'Cairns', 'Toowoomba', 'Mackay', 'Rockhampton']
    awst_list = ['Perth', 'Bunbury']
    
    for town in aest_list:
        df = df.withColumn('timezone', f.when(f.col('city') == town, 'Australia/Brisbane').otherwise(f.col('timezone')))
    for town in awst_list:
        df = df.withColumn('timezone', f.when(f.col('city') == town, 'Australia/Perth').otherwise(f.col('timezone'))) 
    df = df.withColumn('timezone', f.when(f.col('city') == 'Adelaide', 'Australia/Adelaide').otherwise(f.col('timezone')))
    df = df.withColumn('timezone', f.when(f.col('city') == 'Darwin', 'Australia/Darwin').otherwise(f.col('timezone')))

    df = df.withColumn('local_time', f.from_utc_timestamp(f.col('ts_utc0'),f.col('timezone')))
    return df