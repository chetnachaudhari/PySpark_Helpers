import timeit, datetime

from pyspark.sql.functions import col, count, when, concat_ws, collect_list, isnan, explode_outer
from pyspark.sql.types import StructType, ArrayType


def get_list_from_df(input_df):
    return input_df.distinct().rdd.flatMap(lambda x: x).collect()


def get_from_column_with_pattern(input_df, col_name, pattern):
    return input_df.where(col(col_name)
                          .contains(pattern))


def get_groupby_count(input_df, col_name):
    return input_df.groupBy(col_name).count()


def check_it(df):
    print("Count = ", str(df.count()))
    print("Uniq count = ", str(df.dropDuplicates().count()))
    df.select([count(when(col(c).isNotNull(), c)).alias(c) for c in df.columns]).show()


def list_diff(list1, list2):
    return list(set(list1) - set(list2))


def get_count(input_df):
    return input_df.count()


def get_distinct(input_df, input_col):
    return input_df.select(input_col).distinct()


def get_first_element(input_df):
    return input_df.collect()[0][0]


def drop_columns(input_df, columns_to_drop):
    tempDf = input_df.drop(*columns_to_drop)
    return tempDf


def debug_dataframe(input_df, name):
    ##display(inputDf.limit(10))
    start = timeit.default_timer()
    count = input_df.count()
    end = timeit.default_timer()
    execution_time = end - start
    distinct_count = input_df.distinct().count()
    number_of_columns = len(input_df.columns)
    print('Time: {}, Name: {}, Count: {}, Distinct Count: {}, '
          'Execution Time: {}, Number Of Columns: {}'.format(
        datetime.datetime.now(), name, count, distinct_count, execution_time, number_of_columns))


def is_null_or_empty(obj):
    if obj is None:
        return True
    elif type(obj) is str and str(obj).strip().__eq__(''):
        return True
    else:
        return False


def not_none(elem):
    """Check if an element is not None."""
    return elem is not None


def transpose_dataframe(df, columns, pivotCol):
    columnsValue = list(map(lambda x: str("'") + str(x) + str("',")  + str(x), columns))
    stackCols = ','.join(x for x in columnsValue)
    df_1 = df.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")")\
           .select(pivotCol, "col0", "col1")
    final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1"))))\
                 .withColumnRenamed("col0", pivotCol)
    return final_df


def get_completeness(df):
    output_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    return output_df


def flatten(df):
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        # if StructType then convert all sub element to columns.
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        elif (type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df
