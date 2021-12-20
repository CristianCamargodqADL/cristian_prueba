import boto3
import pickle
import logging
import datetime
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s: %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
    handlers=[logging.StreamHandler()],
)
LOGGER = logging.getLogger()


def get_today():
    """
    Get the string of today
    :return: the string of today as format yyyy/mm/dd
    """
    today = datetime.date.today()
    return today.strftime("%Y/%m/%d")


def s3client():
    return boto3.client("s3")


def s3resource():
    return boto3.resource("s3")


def save_model(model: object, s3_url: str):
    """
    Apartir de una ruta abre un objeto pickel
    :param bucket: bucket de trabajo
    :param key: nombre del archivo a referenciar(key de archivo)
    :param pickle_byte_obj: objeto pickel a abrir
    """
    try:
        pickle_byte_obj = pickle.dumps(obj=model)
        bucket = s3_url.split("/")[2]
        key = s3_url.split("/")[3:]
        key = "/".join(key)
        s3resource().Object(bucket, key).put(Body=pickle_byte_obj)
    except Exception as e:
        print(e)
        return False
    return True


def load_model(s3_url: str):
    """
    carga un pkl de S3
    :bucket: bucket donde esta el pkl
    :param input_path: nombre del archivo a referenciar(key de archivo)
    """
    bucket = s3_url.split("/")[2]
    key = s3_url.split("/")[3:]
    key = "/".join(key)
    response = s3client().get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    model = pickle.loads(body)
    return model

def set_spark_session(appname: str):
    spark = SparkSession.builder.appName(appname).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_parquet(path, spark):
    return spark.read.parquet(path[0])

def export_csv(dataframe, path_salida):
    return dataframe.to_csv(path_salida + ".csv",sep = '|',decimal='.',index = False)
 