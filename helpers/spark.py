from os import environ, listdir, path
from json import loads,load
from pprint import pprint

from pyspark import SparkFiles, SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from src.helpers import logging


def start_spark(app_name='my_spark_app', conf_file = "", spark_config={}):
    flag_local = True if 'PYTEST' in environ.keys() else False

    if not flag_local:
        # get Spark session factory
        print("Client or Cluster mode launched")
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))

        # create session and retrieve Spark logger object
        spark_session = spark_builder.getOrCreate()

        return spark_session

    else:
        # get Spark session factory
        print("Local[] mode (client) launched for test purpose")
        spark_builder = (
            SparkSession
            .builder
            .master("local[*]")
            .appName(app_name))
        # create session and retrieve Spark logger object
        spark_session = spark_builder.getOrCreate()
        
        return spark_session
    


def initiate_logger(deploy_mode, spark_session):
    if deploy_mode == "client":
        spark_logger = logging.Log4j(spark_session)
        spark_logger.info("Configure CLIENT logger")
        return spark_logger
    elif deploy_mode == "cluster":
        spark_logger = logging.YarnLogger()
        #YarnLogger.setup_logger()
        spark_logger.info("Configure CLUSTER logger")
        return logging.YarnLogger()

def read_config_file(deploy_mode, config_path, spark_context):
    if deploy_mode == "client":
        with open(config_path.split(':')[1], 'r') as config_file:
            config_json = config_file.read().replace('\n', '')
        config_dict = loads(config_json)
        return config_dict
        
    elif deploy_mode == "cluster":
        config_name = path.basename(config_path)
        conf_file_path_hdfs = environ['SPARK_YARN_STAGING_DIR']+'/'+config_name
        my_RDD_strings = spark_context.wholeTextFiles(conf_file_path_hdfs).values()
        my_RDD_dictionaries = my_RDD_strings.map(loads)
        config_dict = my_RDD_dictionaries.collect()[0]
        return config_dict



        

