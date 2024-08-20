## Basic Idea: sparked wrapper will ensure that spark session is runniong all the timme.
# This class Spark Data Preprocessor will be used to preprocess the data. and run method will run all the methods in sequence as defined in run method.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType, IntegerType, DoubleType
from functools import wraps


def sparked(func):
    """Make sure that spak session is initialized before calling any method"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, 'spark'):
            self._initialize_spark_session()
        return func(self, *args, **kwargs)

    return wrapper


class SparkDataPreprocessor:
    def __init__(self, file_path: str, access_key: str, secret_key: str, endpoint: str = "s3.amazonaws.com"):
        self.file_path = file_path
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint
        self.df = None

    def _initialize_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("S3DataProcessing") \
            .config("spark.hadoop.fs.s3a.access.key", self.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", self.endpoint) \
            .getOrCreate()
        self.df = self.spark.read.parquet(self.file_path)

    @sparked
    def show_data(self, n=5):
        self.df.show(n)

    @sparked
    def drop_columns(self, cols: list):
        self.df = self.df.drop(*cols)

    @sparked
    def fill_missing(self, col_name: str, value):
        self.df = self.df.na.fill(value, subset=[col_name])

    @sparked
    def drop_missing(self, threshold: int = 1):
        self.df = self.df.na.drop(thresh=threshold)

    @sparked
    def filter_data(self, condition: str):
        self.df = self.df.filter(condition)

    @sparked
    def cast_column(self, col_name: str, new_type):
        if new_type == "string":
            new_type = StringType()
        elif new_type == "int":
            new_type = IntegerType()
        elif new_type == "double":
            new_type = DoubleType()

        self.df = self.df.withColumn(col_name, col(col_name).cast(new_type))

    @sparked
    def replace_values(self, col_name: str, to_replace, value):
        self.df = self.df.withColumn(col_name, when(col(col_name) == to_replace, value).otherwise(col(col_name)))

    @sparked
    def save_data(self, output_path: str, format: str = "parquet"):
        if format == "parquet":
            self.df.write.parquet(output_path)
        elif format == "csv":
            self.df.write.csv(output_path)
        elif format == "json":
            self.df.write.json(output_path)

    @sparked
    def get_data(self):
        return self.df
    @sparked
    def run(self):
        self.show_data()
        self.drop_columns(["col1", "col2"])
        self.fill_missing("col3", 0)
        self.drop_missing(1)
        self.filter_data("col4 > 0")
        self.cast_column("col5", "int")
        self.replace_values("col6", "yes", 1)
        self.save_data("s3://bucket-name/output", "parquet")
