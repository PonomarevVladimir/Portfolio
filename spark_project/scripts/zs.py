import sys
import time
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window as w
from project_functions import dist, input_paths, add_local_time

class ZoneStats:
    
    def __init__(self, spark, date, depth, write_mode, path):
        self.spark = spark
        self.events_paths = input_paths(path, date, depth)
        self.oper_dt = date
        self.users_path = f"{path}/first_vitrina"
        self.towns_path = path
        self.write_mode = write_mode
        
    def prepare_towns(self):
        all_window = w.orderBy('tmp')
        
        df_events = self.spark.read.parquet(*self.events_paths).withColumn('tmp', f.lit(1)) \
                              .withColumn('rn', f.row_number().over(all_window)) \
                              .withColumn('user_id', f.coalesce('event.user', 'event.message_from', 'event.reaction_from')) \
                              .withColumn('ts_utc0', f.to_timestamp(f.coalesce('event.datetime', 'event.message_ts'))) 
        
        df_good_events = df_events.filter(f.col('lat').isNotNull()).filter(f.col('lon').isNotNull())

        df_bad_events = df_events.filter(f.col('lat').isNull() | f.col('lon').isNull())
        
        towns = self.spark.read.csv(self.towns_path, header = True, sep = ';').withColumn('tmp', f.lit(1)) \
                                                                              .withColumn('lat2', f.regexp_replace('lat', ',', '.').cast(t.DoubleType())) \
                                                                              .withColumn('lon2', f.regexp_replace('lng', ',', '.').cast(t.DoubleType())) \
                                                                              .select('tmp', f.col('id').alias('zone_id'), 'city', 'lat2', 'lon2')
        
        df_users_loc = self.spark.read.parquet(self.users_path).select('user_id', f.col('act_city').alias('city')) \
                                                               .join(towns, on = 'city' ,how = 'left') \
                                                               .select('user_id', 'city', 'zone_id')
        
        dist_udf = f.udf(dist, t.DoubleType())

        window = w.partitionBy('rn').orderBy('dist')

        df_good_towns = df_good_events.join(towns, on = 'tmp' ,how = 'full') \
                                      .withColumn('dist', dist_udf(f.col('lat'), f.col('lon'), f.col('lat2'), f.col('lon2'))) \
                                      .withColumn('nearest', f.row_number().over(window)) \
                                      .filter(f.col('nearest') == 1) \
                                      .select( 'zone_id'
                                              ,'city'
                                              ,'event_type'
                                              ,'user_id'
                                              ,'ts_utc0')
        df_bad_towns = df_bad_events.join(df_users_loc, on = 'user_id' ,how = 'left') \
                                    .select( 'zone_id'
                                            ,'city'
                                            ,'event_type'
                                            ,'user_id'
                                            ,'ts_utc0')
        
        self.df_towns = df_good_towns.union(df_bad_towns)
        
    def prepare_zone_stat(self):
        window_zone_week_event_type = w.partitionBy('zone_id', 'year', 'week', 'event_type')
        window_zone_month_event_type = w.partitionBy('zone_id', 'month', 'event_type')
        
        df = add_local_time(self.df_towns).withColumn('month', f.date_format('local_time', 'yyyy-MM')) \
                                          .withColumn('year', f.date_format('local_time', 'yyyy')) \
                                          .withColumn('week', f.weekofyear(f.date_format('local_time', 'yyyy-MM-dd'))) \
                                          .withColumn('week_num', f.count('user_id').over(window_zone_week_event_type)) \
                                          .withColumn('month_num', f.count('user_id').over(window_zone_month_event_type)) \
                                          .withColumn('week_user', f.size(f.collect_set('user_id').over(window_zone_week_event_type))) \
                                          .withColumn('month_user', f.size(f.collect_set('user_id').over(window_zone_month_event_type))) \
                                          .select( 'month'
                                                   ,'week'
                                                   ,'zone_id'
                                                   ,'event_type'
                                                   ,'week_num'
                                                   ,'month_num'
                                                   ,'week_user'
                                                   ,'month_user').distinct()
        

        df_usr = df.select('month','week','zone_id','week_user','month_user')
        df_mes = df.select('month','week','zone_id','event_type','week_num','month_num') \
                   .filter(f.col('event_type') == 'message') \
                   .select('month','week','zone_id',f.col('week_num').alias('week_message'),f.col('month_num').alias('month_message'))
        df_rea = df.select('month','week','zone_id','event_type','week_num','month_num') \
                   .filter(f.col('event_type') == 'reaction') \
                   .select('month','week','zone_id',f.col('week_num').alias('week_reaction'),f.col('month_num').alias('month_reaction'))
        df_sub = df.select('month','week','zone_id','event_type','week_num','month_num') \
                   .filter(f.col('event_type') == 'subscription') \
                   .select('month','week','zone_id',f.col('week_num').alias('week_subscription'),f.col('month_num').alias('month_subscription'))

        df_vitrina = df_usr.join(df_mes, on = ['month','week','zone_id'], how = 'full') \
                           .join(df_rea, on = ['month','week','zone_id'], how = 'full') \
                           .join(df_sub, on = ['month','week','zone_id'], how = 'full') \
                           .fillna(0)

        df_vitrina = df_vitrina.select( 'month'
                                       ,'week'
                                       ,'zone_id'
                                       ,'week_message'
                                       ,'week_reaction'
                                       ,'week_subscription'
                                       ,'week_user'
                                       ,'month_message'
                                       ,'month_reaction'
                                       ,'month_subscription'
                                       ,'month_user')

        if self.write_mode == 'append':
            month = self.oper_dt[0:6]
            week = time.strftime(time.strptime(self.oper_dt, 'YYYY-mm-dd'), '%U')
            df_vitrina = df_vitrina.filter((f.col('month') == month) & (f.col('week') == week))
        
        df_vitrina.write.format('parquet').mode(self.write_mode).save(f"{self.towns_path}/second_vitrina") 

def prepare_zs(spark, date, depth, write_mode, path):
    zs = ZoneStats(spark, date, depth, write_mode, path)
    zs.prepare_towns()
    zs.prepare_zone_stat()

if __name__ == "__main__":
    oper_dt = sys.argv[0]
    depth = int(sys.argv[1])
    write_mode = sys.argv[2]
    path = sys.argv[3]

    conf = SparkConf().setAll([ ("spark.driver.memory", "4g")
                               ,("spark.driver.cores", 4)
                               ,("spark.executor.instances","20")
                               ,("spark.executor.cores", 4)
                               ,("spark.executor.memory", "4g")])

    spark = SparkSession.builder \
                        .config(conf=conf) \
                        .appName("zone_stats") \
                        .getOrCreate()

    prepare_zs(spark, oper_dt, depth, write_mode, path)