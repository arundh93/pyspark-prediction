from pyspark.sql import Window
from pyspark.sql.functions import rank, row_number


def delete_duplicates(df):
    window = Window.partitionBy('date_time').orderBy('date_time')
    f1df = df.withColumn('rank', row_number().over(window))
    f2df = f1df.filter(f1df['rank']  == 1)
    return f2df
