from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

import dbldatagen as dg

# examples of generating date time data

# build spark session

# global spark
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Word Count") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

row_count = 1000 * 100
testDataSpec = (dg.DataGenerator(spark, name="test_data_set1", rows=row_count,
                                 partitions=4, randomSeedMethod='hash_fieldname',
                                 verbose=True)
                .withColumn("purchase_id", IntegerType(), minValue=1000000, maxValue=2000000)
                .withColumn("product_code", IntegerType(), uniqueValues=10000, random=True)
                .withColumn("purchase_date", "date", data_range=dg.DateRange("2017-10-01 00:00:00",
                                                                             "2018-10-06 11:55:00",
                                                                             "days=3"),
                            random=True)
                .withColumn("return_date", "date",
                            expr="date_add('purchase_date', cast(floor(rand() * 100 + 1) as int))")

                )

dfTestData = testDataSpec.build()
