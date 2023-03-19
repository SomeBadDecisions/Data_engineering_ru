import pyspark.sql.functions as F 
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from tools import get_spark_session, write_dm
import sys

date = sys.argv[1]

spark = get_spark_session('dm_users')


events_path = "/user/master/data/geo/events" 
geo_path = "/user/konstantin/data/geo_timezone.csv"

events_geo = spark.read.parquet(events_path) \
    .where("event_type == 'message'")\
    .withColumnRenamed("lat", "msg_lat")\
    .withColumnRenamed("lon","msg_lon")\
    .withColumn('user_id', F.col('event.message_from'))\
    .withColumn('event_id', F.monotonically_increasing_id())

geo = spark.read.csv(geo_path, sep=';', header= True)\
      .withColumnRenamed("lat", "city_lat")\
      .withColumnRenamed("lng", "city_lon")

def get_city(events_geo, geo):

    EARTH_R = 6371

    calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.radians(F.col("msg_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
                F.cos(F.radians(F.col("msg_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
                F.pow(F.sin((F.radians(F.col("msg_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
            )
        )

    window = Window().partitionBy('user_id').orderBy(F.col('diff').asc())
    events = events_geo \
            .crossJoin(geo) \
            .withColumn('diff', calculate_diff) \
            .withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col('row_number')==1) \
            .drop('row_number') 
    

    return events

events = get_city(
    events_geo=events_geo,
    geo=geo
)

window_act_city = Window().partitionBy('user_id').orderBy(F.col("date").desc())
act_city = events \
            .withColumn("row_number", F.row_number().over(window_last_city)) \
            .filter(F.col("row_number")==1) \
            .withColumnRenamed('city', 'act_city')

window_travel = Window().partitionBy('user_id', 'id').orderBy(F.col('date'))
travels = events \
    .withColumn("dense_rank", F.dense_rank().over(window_travel)) \
    .withColumn("date_diff", F.datediff(F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), 'd'))) \
    .selectExpr('date_diff', 'user_id', 'date', "id", "city" ) \
    .groupBy("user_id", "date_diff", "id", "city") \
    .agg(F.countDistinct(F.col('date')).alias('cnt_days'))

travels_array = travels.groupBy("user_id") \
    .agg(F.collect_list('city').alias('travel_array')) \
    .select('user_id', 'travel_array', F.size('travel_array').alias('travel_count'))

home = travels.filter((F.col('cnt_days')>27)) \
    .withColumn('max_dt', F.max(F.col('date_diff')).over(Window().partitionBy('user_id')))\
    .where(F.col('date_diff') == F.col('max_dt')) \
    .selectExpr('user_id', 'city as home_city')

def calc_local_tm(events):    
    return events.withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
        .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
        .select("local_time", 'user_id')

local_time = calc_local_tm(act_city)

result = events \
        .join(act_city, ['user_id'], 'left') \
        .join(travels_array,['user_id'], 'left') \
        .join(home, ['user_id'], 'left') \
        .join(local_time, ['user_id'], 'left') \
        .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')

write_df(result, 'dm_users', date)