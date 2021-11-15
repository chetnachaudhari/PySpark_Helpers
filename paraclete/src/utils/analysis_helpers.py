import timeit, datetime

from pyspark.sql.functions import col, count, when


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
