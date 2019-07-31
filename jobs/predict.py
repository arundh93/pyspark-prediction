from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, StructField
import preprocess.sanitize as s
import preprocess.filter as f


def main():
    path = '../data/Train.csv'
    spark: SparkSession = SparkSession.builder.master('local[1]').appName('test app').getOrCreate()
    schema: StructType = StructType([StructField('date_time', StringType(), True),
                                     StructField('is_holiday', StringType(), True),
                                     StructField('air_pollution_index', IntegerType(), True),
                                     StructField('humidity', IntegerType(), True),
                                     StructField('wind_speed', IntegerType(), True),
                                     StructField('wind_direction', IntegerType(), True),
                                     StructField('visibility_in_miles', IntegerType(), True),
                                     StructField('dew_point', IntegerType(), True),
                                     StructField('temperature', DoubleType(), True),
                                     StructField('rain_p_h', IntegerType(), True),
                                     StructField('snow_p_h', IntegerType(), True),
                                     StructField('clouds_all', IntegerType(), True),
                                     StructField('weather_type', StringType(), True),
                                     StructField('weather_description', StringType(), True),
                                     StructField('traffic_volume', IntegerType(), True)])
    df = spark.read.option('header', 'true').schema(schema).csv(path)
    print('---------- initial count ------- %d', df.select('date_time').count())
    fdf: DataFrame = f.delete_duplicates(df)
    fdf1: DataFrame = s.sanitize(fdf)
    # print('---------- final count ------- %d', df.select('date_time').distinct().count())
    print('---------- final count ------- %d', fdf.count())
    fdf1.show()


if __name__ == '__main__':
    main()
