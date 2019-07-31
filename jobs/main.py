from pyspark.sql import SparkSession
from apple import f


def main():
    spark = SparkSession.builder.master('local[*]').appName('test app').getOrCreate()
    rdd = spark.sparkContext.parallelize(f.blah())
    rdd.repartition(1).saveAsTextFile('test1.csv')


if __name__ == '__main__':
    main()
