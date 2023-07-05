import sys
from datetime import datetime
import datetime as dtm
import math as m
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window as w
from project_functions import dist, input_paths, add_local_time

class UserLocks:

    def __init__(self, spark, date, depth, write_mode, path):
        self.spark = spark
        self.towns_path = path
        self.events_paths = input_paths(path, date, depth)
        self.oper_dt = date
        self.home_days = 27
        self.write_mode = write_mode

    def prepare_towns(self):
        all_window = w.orderBy('tmp')
        df_events = self.spark.read.parquet(*self.events_paths).withColumn('tmp', f.lit(1)).withColumn('rn', f.row_number().over(all_window)) \
                                                               .filter(f.col('lat').isNotNull()).filter(f.col('lon').isNotNull()).filter(f.col('event_type') == 'message')
        towns = self.spark.read.csv(self.towns_path, header = True, sep = ';').withColumn('tmp', f.lit(1)) \
                                                                              .withColumn('lat2', f.regexp_replace('lat', ',', '.').cast(t.DoubleType())) \
                                                                              .withColumn('lon2', f.regexp_replace('lng', ',', '.').cast(t.DoubleType())) \
                                                                              .select('tmp', 'city', 'lat2', 'lon2')

        dist_udf = f.udf(dist, t.DoubleType())

        window = w.partitionBy('rn').orderBy('dist')

        self.df_towns = df_events.join(towns, on = 'tmp' ,how = 'full') \
                                 .withColumn('dist', dist_udf(f.col('lat'), f.col('lon'), f.col('lat2'), f.col('lon2'))) \
                                 .withColumn('nearest', f.row_number().over(window)) \
                                 .filter(f.col('nearest') == 1) \
                                 .select( f.col('event.message_from').alias('user_id')
                                         ,f.col('event.message_ts').alias('ts_utc0')
                                         ,f.col('city'))

    def prepare_homes_n_travels(self):
        window_city = w.partitionBy('user_id', 'city')
        window_city_ord = w.partitionBy('user_id', 'city').orderBy(f.col('date').desc())
        window_city_ranged = w.partitionBy('user_id', 'city').orderBy(f.col('date').desc()).rowsBetween(w.currentRow, self.home_days-1)
        window_home = w.partitionBy('user_id').orderBy(f.col('date').desc())

        df_home = self.df_towns.withColumn('date', f.to_date('ts_utc0')).select('user_id', 'date', 'city').distinct() \
                               .withColumn('prev_day_flg', f.when(f.lead('date').over(window_city_ord) == f.date_add('date', -1), 1).otherwise(-1)) \
                               .withColumn('home_flg', f.when(f.min('prev_day_flg').over(window_city_ranged) == 1, 1).otherwise(-1)).filter(f.col('home_flg') == 1) \
                               .withColumn('rnum', f.row_number().over(window_home)).filter(f.col('rnum') == 1) \
                               .select('user_id', f.col('city').alias('home_city'))

        window_time_ord = w.partitionBy('user_id').orderBy('ts_utc0')

        df_travels = self.df_towns.join(df_home, on = "user_id", how = "inner") \
                                  .filter(f.col('city') != f.col('home_city'))  \
                                  .withColumn('prev_town', f.lag('city').over(window_time_ord)) \
                                  .withColumn('trip_flag', f.when(f.col('city') == f.col('prev_town'), 0).otherwise(1)) \
                                  .filter(f.col('trip_flag') == 1) \
                                  .withColumn('travel_count', f.sum('trip_flag').over(window_time_ord)) \
                                  .select('user_id', 'city', 'home_city', 'travel_count')

        df_homes_n_trips = df_travels.select('user_id', 'home_city', 'travel_count').distinct().withColumn('travel_str', f.lit(''))

        user_list = df_homes_n_trips.select('user_id').rdd.flatMap(lambda x: x).collect()
                           
        for user in user_list:
            travel_list = df_travels.filter(f.col('user_id') == user).select('city').rdd.flatMap(lambda x: x).collect()
            travel_str = ''
            for i in range(len(travel_list)):
                travel_str = travel_str + travel_list[i] +','
            df_homes_n_trips = df_homes_n_trips.withColumn('travel_str', f.when(f.col('user_id') == user, travel_str).otherwise(f.col('travel_str')))     

        self.df_homes_n_travels = df_homes_n_trips.withColumn('travel_array', f.split(f.col('travel_str'),','))   

    def prepare_user_locations(self):
        window_time_desc = w.partitionBy('user_id').orderBy(f.col('ts_utc0').desc())

        df_vitrina = self.df_towns.withColumn('row_num', f.row_number().over(window_time_desc)).filter(f.col('row_num') == 1).drop('row_num') \
                                  .join(self.df_homes_n_travels, on = 'user_id', how = 'inner') 

        df_vitrina = add_local_time(df_vitrina).withColumnRenamed('city', 'act_city') \
                                               .select('user_id', 'act_city', 'local_time', 'home_city', 'travel_count', 'travel_array')
        df_vitrina.write.format('parquet').mode(self.write_mode).save(f"{self.towns_path}/first_vitrina") 

def prepare_ul(spark, date, depth, write_mode, path):
    ul = UserLocks(spark,  date, depth, write_mode, path)
    ul.prepare_towns()
    ul.prepare_homes_n_travels()
    ul.prepare_user_locations()

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
                        .appName("user_locs") \
                        .getOrCreate()

    prepare_ul(spark, oper_dt, depth, write_mode, path)