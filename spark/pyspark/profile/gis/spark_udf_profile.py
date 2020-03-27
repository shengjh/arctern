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

import os
import sys
import time

from arctern_pyspark import register_funcs
from pyspark.sql import SparkSession

# add the SPARK_HOME to env
# os.environ["SPARK_HOME"] = "/home/shengjh/Apps/spark-3.0.0-preview2"

profile_dump_path = "/tmp/pyspark-profile"
rows = 10000
test_name = []


def timmer(fun1):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        res = fun1(*args, **kwargs)
        stop_time = time.time()
        dur = stop_time - start_time
        print('run time is %s' % dur)
        with open(os.path.join(profile_dump_path, "timmer.txt"), "a") as f:
            f.write(args[1].split(" ")[1] + " ")
            f.write(str(dur) + "\n")
        return res
    return wrapper


@timmer
def count_and_uncache(spark, sql):
    df = spark.sql(sql)
    df.createOrReplaceTempView("df")
    spark.sql("CACHE TABLE df")
    start_time = time.time()
    spark.sql("UNCACHE TABLE df")
    stop_time = time.time()
    dur = stop_time - start_time
    print("uncahce cost time is %s ", dur)


def run_st_point(spark):
    points_data = []
    points_data.extend([(0.1, 0.1)] * rows)
    points_df = spark.createDataFrame(data=points_data, schema=["x", "y"]).cache()
    points_df.createOrReplaceTempView("points")
    points_df.show()
    count_and_uncache(spark, "select ST_Point(x, y) from points")


def run_st_geomfromgeojson(spark):
    test_data = []
    test_data.extend([("{\"type\":\"Point\",\"coordinates\":[1,2]}",)])
    test_data.extend([("{\"type\":\"LineString\",\"coordinates\":[[1,2],[4,5],[7,8]]}",)])
    test_data.extend([("{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}",)])
    test_data.extend(test_data * rows)
    json_df = spark.createDataFrame(data=test_data, schema=["json"]).cache()
    json_df.createOrReplaceTempView("json")
    sql = "select ST_GeomFromGeoJSON(json) from json"
    json_df.show()
    count_and_uncache(spark, sql)


def run_st_pointfromtext(spark):
    test_data = []
    test_data.extend([('POINT (30 10)',)] * rows)
    data_df = spark.createDataFrame(data=test_data, schema=["data"]).cache()
    data_df.createOrReplaceTempView("data")
    data_df.show()
    sql = "select ST_PointFromText(data) from data"
    count_and_uncache(spark, sql)


def run_st_polygonfromtext(spark):
    test_data = []
    test_data.extend([('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))',)] * rows)
    data_df = spark.createDataFrame(data=test_data, schema=["data"]).cache()
    data_df.createOrReplaceTempView("data")
    data_df.show()
    sql = "select ST_PolygonFromText(data) from data"
    count_and_uncache(spark, sql)


def run_st_astext(spark):
    test_data = []
    test_data.extend([('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))',)] * rows)
    data_df = spark.createDataFrame(data=test_data, schema=["data"]).cache()
    data_df.createOrReplaceTempView("data")
    data_df.show()
    sql = "select ST_AsText(ST_PolygonFromText(data)) from data"
    count_and_uncache(spark, sql)


def run_st_precision_reduce(spark):
    test_data = []
    test_data.extend([('POINT (10.777 11.888)',)] * rows)
    precision_reduce_df = spark.createDataFrame(data=test_data, schema=["geos"]).cache()
    precision_reduce_df.createOrReplaceTempView("precision_reduce")
    precision_reduce_df.show()
    sql = "select ST_PrecisionReduce(geos, 4) from precision_reduce"
    count_and_uncache(spark, sql)


def run_st_linestringfromtext(spark):
    test_data = []
    test_data.extend([('LINESTRING (0 0, 0 1, 1 1, 1 0)',)] * rows)
    data_df = spark.createDataFrame(data=test_data, schema=["data"]).cache()
    data_df.createOrReplaceTempView("data")
    data_df.show()
    sql = "select ST_LineStringFromText(data) from data"
    count_and_uncache(spark, sql)


def run_st_geomfromwkt(spark):
    test_data = []
    test_data.extend([('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))',)] * rows)
    data_df = spark.createDataFrame(data=test_data, schema=["data"]).cache()
    data_df.createOrReplaceTempView("data")
    data_df.show()
    sql = "select ST_GeomFromWKT(data) from data"
    count_and_uncache(spark, sql)


def run_st_geomfromtext(spark):
    test_data = []
    test_data.extend([('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))',)] * rows)
    data_df = spark.createDataFrame(data=test_data, schema=["data"]).cache()
    data_df.createOrReplaceTempView("data")
    data_df.show()
    sql = "select ST_GeomFromText(data) from data"
    count_and_uncache(spark, sql)


def run_st_intersection(spark):
    test_data = []
    test_data.extend([('POINT(0 0)', 'LINESTRING ( 2 0, 0 2 )')] * rows)
    intersection_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    intersection_df.createOrReplaceTempView("intersection")
    intersection_df.show()
    sql = "select ST_Intersection(left, right) from intersection"
    count_and_uncache(spark, sql)


def run_st_isvalid(spark):
    test_data = []
    test_data.extend([('POINT (30 10)',)] * rows)
    valid_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    valid_df.createOrReplaceTempView("valid")
    valid_df.show()
    sql = "select ST_IsValid(geos) from valid"
    count_and_uncache(spark, sql)


def run_st_equals(spark):
    test_data = []
    test_data.extend([('LINESTRING(0 0, 10 10)', 'LINESTRING(0 0, 5 5, 10 10)')] * rows)
    equals_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    equals_df.createOrReplaceTempView("equals")
    equals_df.show()
    sql = "select ST_Equals(left, right) from equals"
    count_and_uncache(spark, sql)


def run_st_touches(spark):
    test_data = []
    test_data.extend([('LINESTRING(0 0, 1 1, 0 2)', 'POINT(1 1)')] * rows)
    touches_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    touches_df.createOrReplaceTempView("touches")
    touches_df.show()
    sql = "select ST_Touches(left, right) from touches"
    count_and_uncache(spark, sql)


def run_st_overlaps(spark):
    test_data = []
    test_data.extend([('POLYGON((1 1, 4 1, 4 5, 1 5, 1 1))', 'POLYGON((3 2, 6 2, 6 6, 3 6, 3 2))')])
    test_data.extend([('POINT(1 0.5)', 'LINESTRING(1 0, 1 1, 3 5)')])
    test_data.extend(test_data * int(rows / 2))
    overlaps_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    overlaps_df.createOrReplaceTempView("overlaps")
    overlaps_df.show()
    sql = "select ST_Overlaps(left, right) from overlaps"
    count_and_uncache(spark, sql)


def run_st_crosses(spark):
    test_data = []
    test_data.extend([('MULTIPOINT((1 3), (4 1), (4 3))', 'POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))')])
    test_data.extend([('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))', 'POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))')])
    test_data.extend(test_data * int(rows / 2))
    crosses_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    crosses_df.createOrReplaceTempView("crosses")
    crosses_df.show()
    sql = "select ST_Crosses(left, right) from crosses"
    count_and_uncache(spark, sql)


def run_st_issimple(spark):
    test_data = []
    test_data.extend([('POLYGON((1 2, 3 4, 5 6, 1 2))',)])
    test_data.extend([('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)',)])
    test_data.extend(test_data * int(rows / 2))
    simple_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    simple_df.createOrReplaceTempView("simple")
    simple_df.show()
    sql = "select ST_IsSimple(geos) from simple"
    count_and_uncache(spark, sql)


def run_st_geometry_type(spark):
    test_data = []
    test_data.extend([('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)',)])
    test_data.extend([('POINT (30 10)',)])
    test_data.extend(test_data * int(rows / 2))
    geometry_type_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    geometry_type_df.createOrReplaceTempView("geometry_type")
    geometry_type_df.show()
    sql = "select ST_GeometryType(geos) from geometry_type"
    count_and_uncache(spark, sql)


# def run_st_make_valid(spark):
#     test_data = []
#     test_data.extend([('LINESTRING(0 0, 10 0, 20 0, 20 0, 30 0)',)])
#     test_data.extend([('POLYGON((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))',)])
#     test_data.extend(test_data * int(rows / 2))
#     make_valid_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
#     make_valid_df.createOrReplaceTempView("make_valid")
#     sql = "select ST_MakeValid(geos) from make_valid"
#     count_and_uncache(spark, sql)

def run_st_make_valid(spark):
    test_data = []
    test_data.extend([('LINESTRING(0 0, 10 0, 20 0, 20 0, 30 0)',)])
    test_data.extend(test_data * int(rows / 2))
    make_valid_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()

    test_data = []
    test_data.extend([('POLYGON((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))',)])
    test_data.extend(test_data * int(rows / 2))
    make_valid_df.union(spark.createDataFrame(data=test_data, schema=['geos'])).cache()

    make_valid_df.show()
    make_valid_df.createOrReplaceTempView("make_valid")
    sql = "select ST_MakeValid(geos) from make_valid"
    count_and_uncache(spark, sql)


def run_st_simplify_preserve_topology(spark):
    test_data = []
    test_data.extend([(
        'POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33, 46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22, 35 26, 24 39, 8 25))',
    )])
    test_data.extend([(
        'LINESTRING(250 250, 280 290, 300 230, 340 300, 360 260, 440 310, 470 360, 604 286)',
    )])
    test_data.extend(test_data * int(rows / 2))
    simplify_preserve_topology_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    simplify_preserve_topology_df.createOrReplaceTempView("simplify_preserve_topology")
    simplify_preserve_topology_df.show()
    sql = "select ST_SimplifyPreserveTopology(geos, 10) from simplify_preserve_topology"
    count_and_uncache(spark, sql)


def run_st_polygon_from_envelope(spark):
    test_data = []
    test_data.extend([(
        1.0, 3.0, 5.0, 7.0
    )])
    test_data.extend([(
        2.0, 4.0, 6.0, 8.0
    )])
    test_data.extend(test_data * int(rows / 2))
    polygon_from_envelope_df = spark.createDataFrame(data=test_data,
                                                     schema=['min_x', 'min_y', 'max_x', 'max_y']).cache()
    polygon_from_envelope_df.createOrReplaceTempView('polygon_from_envelope')
    polygon_from_envelope_df.show()
    sql = "select ST_PolygonFromEnvelope(min_x, min_y, max_x, max_y) from polygon_from_envelope"
    count_and_uncache(spark, sql)


def run_st_contains(spark):
    test_data = []
    test_data.extend([(
        'POLYGON((-1 3,2 1,0 -3,-1 3))',
        'POLYGON((0 2,1 1,0 -1,0 2))'
    )])
    test_data.extend([(
        'POLYGON((0 2,1 1,0 -1,0 2))',
        'POLYGON((-1 3,2 1,0 -3,-1 3))'
    )])
    test_data.extend(test_data * int(rows / 2))
    contains_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    contains_df.createOrReplaceTempView("contains")
    contains_df.show()
    sql = "select ST_Contains(left, right) from contains"
    count_and_uncache(spark, sql)


def run_st_intersects(spark):
    test_data = []
    test_data.extend([(
        'POINT(0 0)',
        'LINESTRING ( 0 0, 0 2 )'
    )])
    test_data.extend([(
        'POINT(0 0)',
        'LINESTRING ( 2 0, 0 2 )'
    )])
    test_data.extend(test_data * int(rows / 2))
    intersects_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    intersects_df.createOrReplaceTempView("intersects")
    intersects_df.show()
    sql = "select ST_Intersects(left, right) from intersects"
    count_and_uncache(spark, sql)


def run_st_within(spark):
    test_data = []
    test_data.extend([(
        'POLYGON((2 2, 7 2, 7 5, 2 5, 2 2))',
        'POLYGON((1 1, 8 1, 8 7, 1 7, 1 1))'
    )])
    test_data.extend([(
        'POLYGON((0 2, 5 2, 5 5, 0 5, 0 2))',
        'POLYGON((1 1, 8 1, 8 7, 1 7, 1 1))'
    )])
    test_data.extend(test_data * int(rows / 2))
    within_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    within_df.createOrReplaceTempView("within")
    within_df.show()
    sql = "select ST_Within(left, right) from within"
    count_and_uncache(spark, sql)


def run_st_distance(spark):
    test_data = []
    test_data.extend([(
        'POLYGON((-1 -1,2 2,0 1,-1 -1))',
        'POLYGON((5 2,7 4,5 5,5 2))'
    )])
    test_data.extend([(
        'POINT(31.75 31.25)',
        'LINESTRING(32 32,32 35,40.5 35,32 35,32 32)'
    )])
    test_data.extend(test_data * int(rows / 2))
    distance_df = spark.createDataFrame(data=test_data, schema=["left", "right"]).cache()
    distance_df.createOrReplaceTempView("distance")
    distance_df.show()
    sql = "select ST_Distance(left, right) from distance"
    count_and_uncache(spark, sql)


def run_st_area(spark):
    test_data = []
    test_data.extend([('POLYGON((10 20,10 30,20 30,30 10))',)])
    test_data.extend([('POLYGON((10 20,10 40,30 40,40 10))',)])
    test_data.extend(test_data * int(rows / 2))
    area_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    area_df.createOrReplaceTempView("area")
    area_df.show()
    sql = "select ST_Area(geos) from area"
    count_and_uncache(spark, sql)


def run_st_centroid(spark):
    test_data = []
    test_data.extend([('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )',)])
    test_data.extend([('CIRCULARSTRING(0 2, -1 1,0 0, 0.5 0, 1 0, 2 1, 1 2, 0.5 2, 0 2)',)])
    test_data.extend(test_data * int(rows / 2))
    centroid_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    centroid_df.createOrReplaceTempView("centroid")
    centroid_df.show()
    sql = "select ST_Centroid(geos) from centroid"
    count_and_uncache(spark, sql)


def run_st_length(spark):
    test_data = []
    test_data.extend([('LINESTRING(743238 2967416,743238 2967450,743265 2967450, 743265.625 2967416,743238 2967416)',)])
    test_data.extend([('LINESTRING(-72.1260 42.45, -72.1240 42.45666, -72.123 42.1546)',)])
    test_data.extend(test_data * int(rows / 2))
    length_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    length_df.createOrReplaceTempView("length")
    length_df.show()
    sql = "select ST_Length(geos) from length"
    count_and_uncache(spark, sql)


def run_st_hausdorffdistance(spark):
    test_data = []
    test_data.extend([("POLYGON((0 0 ,0 1, 1 1, 1 0, 0 0))", "POLYGON((0 0 ,0 2, 1 1, 1 0, 0 0))",)])
    test_data.extend([("POINT(0 0)", "POINT(0 1)",)])
    test_data.extend(test_data * int(rows / 2))
    hausdorff_df = spark.createDataFrame(data=test_data, schema=["geo1", "geo2"]).cache()
    hausdorff_df.createOrReplaceTempView("hausdorff")
    hausdorff_df.show()
    sql = "select ST_HausdorffDistance(geo1,geo2) from hausdorff"
    count_and_uncache(spark, sql)


def run_st_convexhull(spark):
    test_data = []
    test_data.extend([('GEOMETRYCOLLECTION(POINT(1 1),POINT(0 0))',)])
    test_data.extend([('GEOMETRYCOLLECTION(LINESTRING(2.5 3,-2 1.5), POLYGON((0 1,1 3,1 -2,0 1)))',)])
    test_data.extend(test_data * int(rows / 2))
    convexhull_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    convexhull_df.createOrReplaceTempView("convexhull")
    convexhull_df.show()
    sql = "select ST_convexhull(geos) from convexhull"
    count_and_uncache(spark, sql)


def run_st_npoints(spark):
    test_data = []
    test_data.extend([('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)',)])
    test_data.extend([('LINESTRING(77.29 29.07 1,77.42 29.26 0,77.27 29.31 -1,77.29 29.07 3)',)])
    test_data.extend(test_data * int(rows / 2))
    npoints_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    npoints_df.createOrReplaceTempView("npoints")
    npoints_df.show()
    sql = "select ST_NPoints(geos) from npoints"
    count_and_uncache(spark, sql)


def run_st_envelope(spark):
    test_data = []
    test_data.extend([('point (10 10)',)])
    test_data.extend([('linestring (0 0 , 0 10)',)])
    test_data.extend([('linestring (0 0 , 10 0)',)])
    test_data.extend([('linestring (0 0 , 10 10)',)])
    test_data.extend([('polygon ((0 0, 10 0, 10 10, 0 10, 0 0))',)])
    test_data.extend([('multipoint (0 0, 10 0, 5 5)',)])
    test_data.extend([('multilinestring ((0 0, 5 5), (6 6, 6 7, 10 10))',)])
    test_data.extend([('multipolygon (((0 0, 10 0, 10 10, 0 10, 0 0), (11 11, 20 11, 20 20, 20 11, 11 11)))',)])
    test_data.extend(test_data * int(rows / 8))
    envelope_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    envelope_df.createOrReplaceTempView("envelope")
    envelope_df.show()
    sql = "select ST_Envelope(geos) from envelope"
    count_and_uncache(spark, sql)


def run_st_buffer(spark):
    test_data = []
    test_data.extend([('POLYGON((0 0,1 0,1 1,0 0))',)] * rows)
    buffer_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    buffer_df.createOrReplaceTempView("buffer")
    buffer_df.show()
    sql = "select ST_Buffer(geos, 1.2) from buffer"
    count_and_uncache(spark, sql)


def run_st_union_aggr(spark):
    test_data1 = []
    test_data1.extend([('POLYGON ((1 1,1 2,2 2,2 1,1 1))',)])
    test_data1.extend([('POLYGON ((2 1,3 1,3 2,2 2,2 1))',)])
    test_data1.extend(test_data1 * int(rows / 2))
    union_aggr_df1 = spark.createDataFrame(data=test_data1, schema=['geos']).cache()
    union_aggr_df1.createOrReplaceTempView("union_aggr1")
    union_aggr_df1.show()
    sql = "select ST_Union_Aggr(geos) from union_aggr1"
    count_and_uncache(spark, sql)


# assert rs[0][0] == 'POLYGON ((1 1,1 2,2 2,3 2,3 1,2 1,1 1))'

# test_data2 = []
# test_data2.extend([('POLYGON ((0 0,4 0,4 4,0 4,0 0))',)])
# test_data2.extend([('POLYGON ((3 1,5 1,5 2,3 2,3 1))',)])
# test_data2.extend(test_data2 * int(rows / 2))
# union_aggr_df2 = spark.createDataFrame(data=test_data2, schema=['geos']).cache()
# union_aggr_df2.createOrReplaceTempView("union_aggr2")
# rs = spark.sql("select ST_Union_Aggr(geos) from union_aggr2").cache()
# assert rs[0][0] == 'POLYGON ((4 1,4 0,0 0,0 4,4 4,4 2,5 2,5 1,4 1))'
#
# test_data3 = []
# test_data3.extend([('POLYGON ((0 0,4 0,4 4,0 4,0 0))',)])
# test_data3.extend([('POLYGON ((5 1,7 1,7 2,5 2,5 1))',)])
# union_aggr_df3 = spark.createDataFrame(data=test_data3, schema=['geos']).cache()
# union_aggr_df3.createOrReplaceTempView("union_aggr3")
# rs = spark.sql("select ST_Union_Aggr(geos) from union_aggr3").cache()
# assert rs[0][0] == 'MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((5 1,7 1,7 2,5 2,5 1)))'
#
# test_data4 = []
# test_data4.extend([('POLYGON ((0 0,0 4,4 4,4 0,0 0))',)])
# test_data4.extend([('POINT (2 3)',)])
# union_aggr_df4 = spark.createDataFrame(data=test_data4, schema=['geos']).cache()
# union_aggr_df4.createOrReplaceTempView("union_aggr4")
# rs = spark.sql("select ST_Union_Aggr(geos) from union_aggr4").cache()
# assert rs[0][0] == 'POLYGON ((0 0,0 4,4 4,4 0,0 0))'


def run_st_envelope_aggr(spark):
    test_data = []
    test_data.extend([('POLYGON ((0 0,4 0,4 4,0 4,0 0))',)])
    test_data.extend([('POLYGON ((5 1,7 1,7 2,5 2,5 1))',)])
    test_data.extend(test_data * int(rows / 2))
    envelope_aggr_df = spark.createDataFrame(data=test_data, schema=['geos'])
    envelope_aggr_df.createOrReplaceTempView('envelope_aggr')
    envelope_aggr_df.show()
    sql = "select ST_Envelope_Aggr(geos) from envelope_aggr"
    count_and_uncache(spark, sql)


def run_st_transform(spark):
    test_data = []
    test_data.extend([('POINT (10 10)',)] * rows)
    buffer_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    buffer_df.createOrReplaceTempView("buffer")
    buffer_df.show()
    sql = "select ST_Transform(geos, 'epsg:4326', 'epsg:3857') from buffer"
    count_and_uncache(spark, sql)


def run_st_curvetoline(spark):
    test_data = []
    test_data.extend([('CURVEPOLYGON(CIRCULARSTRING(0 0, 4 0, 4 4, 0 4, 0 0))',)] * rows)
    buffer_df = spark.createDataFrame(data=test_data, schema=['geos']).cache()
    buffer_df.createOrReplaceTempView("buffer")
    buffer_df.show()
    sql = "select ST_CurveToLine(geos) from buffer"
    count_and_uncache(spark, sql)


def parse_args(argv):
    import sys, getopt
    try:
        opts, args = getopt.getopt(argv, "hr:p:t:", ["rows=", "path=", "test name"])
    except getopt.GetoptError:
        print('spark_udf_profile.py -r <rows> -p <dump_path>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('spark_udf_profile.py -r <rows> -p <dump_path>')
            sys.exit()
        elif opt in ("-r", "--rows"):
            global rows
            rows = int(arg)
        elif opt in ("-p", "--path"):
            global profile_dump_path
            profile_dump_path = arg
        elif opt in ("-t", "--test"):
            global test_name
            test_name = arg.split(',')
    profile_dump_path = os.path.join(profile_dump_path, str(rows))
    if not os.path.exists(profile_dump_path):
        os.makedirs(profile_dump_path)


if __name__ == "__main__":
    parse_args(sys.argv[1:])
    spark_session = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark profile") \
        .config("spark.python.profile", "true") \
        .config("spark.python.profile.dump", profile_dump_path) \
        .getOrCreate()

    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    register_funcs(spark_session)

    funcs = {
        'st_point': run_st_point,
        'st_intersection': run_st_intersection,
        'st_isvalid': run_st_isvalid,
        'st_equals': run_st_equals,
        'st_touches': run_st_touches,
        'st_overlaps': run_st_overlaps,
        'st_crosses': run_st_crosses,
        'st_issimple': run_st_issimple,
        'st_geometry_type': run_st_geometry_type,
        'st_make_valid': run_st_make_valid,
        'st_simplify_preserve_topology': run_st_simplify_preserve_topology,
        'st_polygon_from_envelope': run_st_polygon_from_envelope,
        'st_contains': run_st_contains,
        'st_intersects': run_st_intersects,
        'st_within': run_st_within,
        'st_distance': run_st_distance,
        'st_area': run_st_area,
        'st_centroid': run_st_centroid,
        'st_length': run_st_length,
        'st_hausdorffdistance': run_st_hausdorffdistance,
        'st_convexhull': run_st_convexhull,
        'st_npoints': run_st_npoints,
        'st_envelope': run_st_envelope,
        'st_buffer': run_st_buffer,
        'st_union_aggr': run_st_union_aggr,
        'st_envelope_aggr': run_st_envelope_aggr,
        'st_transform': run_st_transform,
        'st_curvetoline': run_st_curvetoline,
        'st_geomfromgeojson': run_st_geomfromgeojson,
        'st_pointfromtext': run_st_pointfromtext,
        'st_polygonfromtext': run_st_polygonfromtext,
        'st_linestringfromtext': run_st_linestringfromtext,
        'st_geomfromwkt': run_st_geomfromwkt,
        'st_geomfromtext': run_st_geomfromtext,
        'st_astext': run_st_astext,
    }

    test_name = test_name or funcs.keys()
    for test in test_name:
        funcs[test](spark_session)

    # run_st_point(spark_session)
    # run_st_intersection(spark_session)
    # run_st_isvalid(spark_session)
    # run_st_equals(spark_session)
    # run_st_touches(spark_session)
    # run_st_overlaps(spark_session)
    # run_st_crosses(spark_session)
    # run_st_issimple(spark_session)
    # run_st_geometry_type(spark_session)
    # run_st_make_valid(spark_session)
    # run_st_simplify_preserve_topology(spark_session)
    # run_st_polygon_from_envelope(spark_session)
    # run_st_contains(spark_session)
    # run_st_intersects(spark_session)
    # run_st_within(spark_session)
    # run_st_distance(spark_session)
    # run_st_area(spark_session)
    # run_st_centroid(spark_session)
    # run_st_length(spark_session)
    # run_st_hausdorffdistance(spark_session)
    # run_st_convexhull(spark_session)
    # run_st_npoints(spark_session)
    # run_st_envelope(spark_session)
    # run_st_buffer(spark_session)
    # run_st_union_aggr(spark_session)
    # run_st_envelope_aggr(spark_session)
    # run_st_transform(spark_session)
    # run_st_curvetoline(spark_session)
    # run_st_geomfromgeojson(spark_session)
    # run_st_pointfromtext(spark_session)
    # run_st_polygonfromtext(spark_session)
    # run_st_linestringfromtext(spark_session)
    # run_st_geomfromwkt(spark_session)
    # run_st_geomfromtext(spark_session)
    # run_st_astext(spark_session)

    spark_session.stop()
