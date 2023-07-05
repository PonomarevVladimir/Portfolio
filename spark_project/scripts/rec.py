import sys
import pyspark
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window as w
from project_functions import dist, input_paths, add_local_time

class FriendRecomendation:
    
    def __init__(self, spark, date, depth, write_mode, path):
        self.spark = spark
        self.events_paths = input_paths(path, date, depth)
        self.towns_path = path
        self.oper_dt = date
        self.write_mode = write_mode
        
    def prepare_user_info(self):
        window_user = w.partitionBy('user')
        self.df_events = self.spark.read.parquet(*self.events_paths) 

        df_user_info_from = self.df_events.select('event.message_from', f.col('event.message_to').alias('user')) \
                                          .distinct() \
                                          .filter(f.col('user').isNotNull()) \
                                          .withColumn('friends_list_from', f.coalesce(f.collect_list('message_from').over(window_user), f.array()))
        
        self.df_user_info = self.df_events.select('event.user', 'event.message_from', 'event.message_to', 'event.subscription_channel') \
                                .distinct() \
                                .withColumn('user', f.coalesce('user', 'message_from')) \
                                .filter(f.col('user').isNotNull()).drop('message_from') \
                                .withColumn('friends_list_to', f.coalesce(f.collect_list('message_to').over(window_user), f.array())) \
                                .join(df_user_info_from, on = 'user', how = 'full') \
                                .withColumn('friends_list', f.array_distinct(f.concat(f.coalesce(f.col('friends_list_to'), f.array()), 
                                                                                      f.coalesce(f.col('friends_list_from'), f.array())))) \
                                .drop('friends_list_to', 'friends_list_from') \
                                .withColumn('subs_list',  f.coalesce(f.collect_list('subscription_channel').over(window_user), f.array())) \
                                .drop('message_to', 'subscription_channel').distinct()
        
    def prepare_user_locs(self):
        all_window = w.orderBy('tmp')
        df_ts = self.df_events.select('event.user', 'event.message_from', 'event.reaction_from', 'event.datetime', 'event.message_ts', 'lat', 'lon') \
                    .withColumn('user', f.coalesce('user', 'message_from', 'reaction_from')) \
                    .withColumn('ts_utc0', f.coalesce('datetime', 'message_ts')) \
                    .select('user', 'ts_utc0', 'lat', 'lon') \
                    .filter(f.col('lat').isNotNull()).filter(f.col('lon').isNotNull()).withColumn('tmp', f.lit(1)) \
                    .withColumn('rn', f.row_number().over(all_window))
                                                             
        towns = self.spark.read.csv(self.towns_path, header = True, sep = ';').withColumn('tmp', f.lit(1)) \
                                                                              .withColumn('lat2', f.regexp_replace('lat', ',', '.').cast(t.DoubleType())) \
                                                                              .withColumn('lon2', f.regexp_replace('lng', ',', '.').cast(t.DoubleType())) \
                                                                              .select('tmp', 'city', 'id', 'lat2', 'lon2')
                                                             
        dist_udf = f.udf(dist, t.DoubleType()) 
        window = w.partitionBy('rn').orderBy('dist')
                                                             
        df_ts = df_ts.join(towns, on = 'tmp' ,how = 'full') \
                      .withColumn('dist', dist_udf(f.col('lat'), f.col('lon'), f.col('lat2'), f.col('lon2'))) \
                      .withColumn('nearest', f.row_number().over(window)) \
                      .filter(f.col('nearest') == 1) \
                      .select('user', 'ts_utc0', 'city', f.col('id').alias('zone_id'), 'lat', 'lon')
                                                             
        window_user_day = w.partitionBy('user', 'oper_dt').orderBy(f.col('local_time').desc())
                                                             
        self.df_locs = add_local_time(df_ts).withColumn('oper_dt', f.date_format('local_time', 'yyyy-MM-dd')) \
                                            .withColumn('rn', f.row_number().over(window_user_day)) \
                                            .filter(f.col('rn') == 1) \
                                            .select('user', 'oper_dt', 'zone_id','timezone', 'lat', 'lon', 'local_time')
                                                            
    def prepare_possible_friends(self):
        dist_udf = f.udf(dist, t.DoubleType()) 
        left_loc = self.df_locs.withColumnRenamed('user', 'left_user') \
                       .withColumnRenamed('lat', 'left_lat') \
                       .withColumnRenamed('lon', 'left_lon') \
                       .withColumnRenamed('local_time', 'local_time_left')
        right_loc = self.df_locs.withColumnRenamed('user', 'right_user') \
                                .withColumnRenamed('lat', 'right_lat') \
                                .withColumnRenamed('lon', 'right_lon') \
                                .withColumnRenamed('local_time', 'local_time_right')
        df_pf = left_loc.join(right_loc, on = ['oper_dt', 'zone_id', 'timezone'], how = 'full') \
                        .filter(f.col('left_user') != f.col('right_user')) \
                        .withColumn('dist', dist_udf(f.col('left_lat'), f.col('left_lon'), f.col('right_lat'), f.col('right_lon'))) \
                        .filter(f.col('dist') <= 1).select('left_user', 'right_user', 'zone_id','timezone', 'oper_dt', 'local_time_left', 'local_time_right')\
                        .withColumn('pair_id', f.array_sort(f.array(f.col('left_user'), f.col('right_user'))))
        self.df_pairs = df_pf.dropDuplicates(subset = ['pair_id'])
        
    def prepare_recomendation(self):
        df_vitrina = self.df_pairs.join(self.df_user_info, self.df_pairs['left_user'] == self.df_user_info['user'], 'left') \
                                  .withColumn('left_friends_list', f.coalesce(f.col('friends_list'), f.array())) \
                                  .withColumn('left_subs_list', f.coalesce(f.col('subs_list'), f.array())) \
                                  .drop('user', 'friends_list', 'subs_list') \
                                  .join(self.df_user_info, self.df_pairs['right_user'] == self.df_user_info['user'], 'left') \
                                  .withColumn('right_subs_list',  f.coalesce(f.col('subs_list'), f.array())) \
                                  .withColumn('right_user_list', f.array(f.col('right_user').cast(t.IntegerType()))) \
                                  .withColumn('subs_intersect', f.size(f.array_intersect(f.col('left_subs_list'), f.col('right_subs_list')))) \
                                  .withColumn('are_friends', f.size(f.array_intersect(f.col('right_user_list'), f.col('left_friends_list')))) \
                                  .filter((f.col('are_friends') == 0) & (f.col('subs_intersect') > 0)) \
                                  .select( f.col('left_user').alias('user_left')
                                          ,f.col('right_user').alias('user_right')
                                          ,f.col('oper_dt').alias('processed_dttm')
                                          ,'zone_id'
                                          ,'timezone'
                                          ,'local_time_left'
                                          ,'local_time_right')
        
        if self.write_mode == 'append':
            df_vitrina = df_vitrina.filter(f.col('processed_dttm') == self.oper_dt)
        
        df_vitrina.write.format('parquet').mode(self.write_mode).save(f"{self.towns_path}/third_vitrina") 

def prepare_rec(spark, date, depth, write_mode, path):
    rec = FriendRecomendation(spark, date, depth, write_mode, path)
    rec.prepare_user_info()
    rec.prepare_user_locs()
    rec.prepare_possible_friends()
    rec.prepare_recomendation()
    
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
                        .appName("recomendations") \
                        .getOrCreate()

    prepare_rec(spark, oper_dt, depth, write_mode, path)