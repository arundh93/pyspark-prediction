from pyspark.sql.functions import col, udf, trunc, ltrim
from pyspark.sql.types import StringType


def splits(s):
    return s.split(' ')[1].split(':')[0]


split_udf = udf(splits, StringType())


def sanitize(df):
    df1 = df.withColumn('date_time', split_udf('date_time'))
    return df1
