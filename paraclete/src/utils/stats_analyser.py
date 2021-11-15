from pyspark.shell import sqlContext
from pyspark.sql.types import StringType, StructType, StructField, BooleanType


def get_list_of_databases():
    return sqlContext.sql("show databases")


def get_table_list_schema():
    schema = StructType([
        StructField('database', StringType(), False),
        StructField('tableName', StringType(), False),
        StructField('isTemporary', BooleanType(), False)
    ])
    return schema


def get_list_of_tables(spark, sc, schema):
    list_of_databases = get_list_of_databases().rdd.collect()
    tables_df = spark.createDataFrame(sc.emptyRDD(), schema)
    for row in list_of_databases:
        db_name = row['databaseName']
        show_table_sql_str = "show tables from {}".format(db_name)
        tables_df = tables_df.union(sqlContext.sql(show_table_sql_str))

    return tables_df


def get_table_row_count(db_name, tbl_name):
    try:
        sql_str = 'select count(*) cnt from {}.{}'.format(db_name, tbl_name)
        output_df = sqlContext.sql(sql_str)
        table_count = output_df.collect()[0]['cnt']
    except:
        table_count = -1
    return table_count


def get_counts_from_all(spark, tables_df):
    list_of_counts = []
    for table in tables_df.rdd.collect():
        db_name = table['database']
        tbl_name = table['tableName']
        table_count = get_table_row_count(db_name, tbl_name)
        list_of_counts.append((db_name, tbl_name, table_count))
        print('{}.{} = {}'.format(db_name, tbl_name, table_count))
    col_names = ['database_name', 'table_name', 'row_count']
    df = spark.createDataFrame(list_of_counts, col_names)

    return df
