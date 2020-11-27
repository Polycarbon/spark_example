
import sys
from helpers.spark import start_spark

HDFS_URI = 'hdfs://edmhad03b:8020'


def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark = start_spark(app_name='my_etl_job')
    df = spark.read.option("multiline", "true").json('data/test1.json')
    df = transform_data(df)
    dump_data(df)


def transform_data(df):
    return df.groupBy(df.group_id).avg('value')


def dump_data(df, path):
    df.write.json(path, mode='overwrite')


# entry point for PySpark ETL application
if __name__ == '__main__':
    sys.exit(main())
