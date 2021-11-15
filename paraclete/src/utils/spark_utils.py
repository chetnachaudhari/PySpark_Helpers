from pyspark.sql.types import StructType, StructField, StringType


def create_database(spark, db_name):
    sql_str = "CREATE DATABASE IF NOT EXISTS " + db_name
    spark.sql(sql_str)
    return sql_str


def create_delta_table(spark, db_name, table_name, delta_path):
    try:
        table_name1 = table_name.replace('.', '_')
        delta_table = '.'.join([db_name, table_name1])
        sql_str = "CREATE TABLE IF NOT EXISTS " + delta_table + " USING DELTA LOCATION '" + delta_path + "'"
        spark.sql(sql_str)
    except Exception as e:
        return f"###Exception:create_delta_table:{str(e)[0:300]}"
    return sql_str


def createEmpty_df(spark):
    schema = StructType([
        StructField('placeHolder', StringType(), True)])
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return df



