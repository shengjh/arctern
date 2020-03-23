# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import SparkSession
from arctern_pyspark import register_funcs


def run_st_point(spark):
    register_funcs(spark)
    # points_df = spark.read.json("/tmp/points.json")
    # points_df.cache()
    data = [(1.9, 9.0)] * 10
    points_df = spark.createDataFrame(
        data,
        ("x", "y"))
    points_df.createOrReplaceTempView("points")
    spark.sql("select ST_Point(x, y) from points").cache().show()
    print(points_df.count())


if __name__ == "__main__":
    spark_session = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .config("spark.python.profile", "true") \
        .config("spark.python.profile.dump", "/tmp/pyspark-profile") \
        .getOrCreate()

    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    run_st_point(spark_session)

    spark_session.sparkContext.show_profiles()

    spark_session.stop()
