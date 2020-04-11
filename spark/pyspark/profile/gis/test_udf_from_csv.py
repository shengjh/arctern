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
import hdfs

from arctern_pyspark import register_funcs, union_aggr, envelope_aggr
from pyspark.sql import SparkSession

# add the SPARK_HOME to env
# os.environ["SPARK_HOME"] = "/home/shengjh/Apps/spark-3.0.0-preview2"

data_path = ""
output_path = ""
test_name = []
hdfs_url = ""
client_hdfs = None
to_hdfs = False
report_file_path = ""


def is_hdfs(path):
    return path.startswith("hdfs://")


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def timmer(fun1):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        res = fun1(*args, **kwargs)
        stop_time = time.time()
        dur = stop_time - start_time
        if to_hdfs:
            with client_hdfs.write(report_file_path, append=True) as f:
                f.write(str.encode(args[0] + " "))
                f.write(str.encode(str(dur) + "\n"))
        else:
            with open(report_file_path, 'w') as f:
                f.write(args[0] + " ")
                f.write(str(dur) + "\n")
        return res

    return wrapper


@timmer
def calculate_with_timmer(func_name, spark, sql):
    df = spark.sql(sql)
    df.createOrReplaceTempView("df")
    spark.sql("CACHE TABLE df")
    spark.sql("UNCACHE TABLE df")


def calculate(spark, sql):
    df = spark.sql(sql)
    df.createOrReplaceTempView("df")
    spark.sql("CACHE TABLE df")
    spark.sql("UNCACHE TABLE df")


@timmer
def calculate_union_agg_with_timmer(func_name, spark, sql):
    df = spark.sql(sql)
    rdf = union_aggr(df, "geos")


def calculate_union_agg(spark, sql):
    df = spark.sql(sql)
    rdf = union_aggr(df, "geos")


@timmer
def calculate_envelope_agg_with_timmer(func_name, spark, sql):
    df = spark.sql(sql)
    rdf = envelope_aggr(df, "geos")


def calculate_envelope_agg(func_name, spark, sql):
    df = spark.sql(sql)
    rdf = envelope_aggr(df, "geos")


def test_log(f):
    def wrapper(*args, **kwargs):
        print("--------Start test", f.__name__ + "--------")
        f(*args, **kwargs)
        print("--------Finish test", f.__name__ + "--------")

    return wrapper


@test_log
def run_st_point(spark):
    file_path = os.path.join(data_path, 'st_point.csv')
    points_df = spark.read.csv(file_path, schema='x double, y double').cache()
    points_df.createOrReplaceTempView("points")
    sql = "select ST_AsText(ST_Point(x, y)) from points"
    calculate(spark, sql)
    calculate_with_timmer('st_point', spark, sql)


@test_log
def run_st_geomfromgeojson(spark):
    file_path = os.path.join(data_path, 'st_geomfromgeojson.csv')
    json_df = spark.read.csv(file_path, schema="json string").cache()
    json_df.createOrReplaceTempView("json")
    sql = "select ST_AsText(ST_GeomFromGeoJSON(json)) from json"
    calculate(spark, sql)
    calculate_with_timmer('st_geomfromgeojson', spark, sql)


@test_log
def run_st_pointfromtext(spark):
    file_path = os.path.join(data_path, 'st_pointfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_AsText(ST_PointFromText(data)) from data"
    calculate(spark, sql)
    calculate_with_timmer('st_pointfromtext', spark, sql)


@test_log
def run_st_polygonfromtext(spark):
    file_path = os.path.join(data_path, 'st_polygonfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_AsText(ST_PolygonFromText(data)) from data"
    calculate(spark, sql)
    calculate_with_timmer('st_polygonfromtext', spark, sql)


@test_log
def run_st_astext(spark):
    file_path = os.path.join(data_path, 'st_astext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_AsText(ST_PolygonFromText(data)) from data"
    calculate(spark, sql)
    calculate_with_timmer('st_astext', spark, sql)


@test_log
def run_st_precision_reduce(spark):
    file_path = os.path.join(data_path, 'st_precision_reduce.csv')
    precision_reduce_df = spark.read.csv(file_path, schema="geos string").cache()
    precision_reduce_df.createOrReplaceTempView("precision_reduce")
    sql = "select ST_AsText(ST_PrecisionReduce(ST_GeomFromText(geos), 4)) from precision_reduce"
    calculate(spark, sql)
    calculate_with_timmer('st_precision_reduce', spark, sql)


@test_log
def run_st_linestringfromtext(spark):
    file_path = os.path.join(data_path, 'st_linestringfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_AsText(ST_LineStringFromText(data)) from data"
    calculate(spark, sql)
    calculate_with_timmer('st_linestringfromtext', spark, sql)


@test_log
def run_st_geomfromwkt(spark):
    file_path = os.path.join(data_path, 'st_geomfromwkt.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_AsText(ST_GeomFromWKT(data)) from data"
    calculate(spark, sql)
    calculate_with_timmer('st_geomfromwkt', spark, sql)


@test_log
def run_st_geomfromtext(spark):
    file_path = os.path.join(data_path, 'st_geomfromtext.csv')
    data_df = spark.read.csv(file_path, schema="data string").cache()
    data_df.createOrReplaceTempView("data")
    sql = "select ST_AsText(ST_GeomFromText(data)) from data"
    calculate(spark, sql)
    calculate_with_timmer('st_geomfromtext', spark, sql)


@test_log
def run_st_intersection(spark):
    file_path = os.path.join(data_path, 'st_intersection.csv')
    intersection_df = spark.read.csv(file_path, schema="left string, right string").cache()
    intersection_df.createOrReplaceTempView("intersection")
    sql = "select ST_AsText(ST_Intersection(ST_GeomFromText(left), ST_GeomFromText(right))) from intersection"
    calculate(spark, sql)
    calculate_with_timmer('st_intersection', spark, sql)


@test_log
def run_st_isvalid(spark):
    file_path = os.path.join(data_path, 'st_isvalid.csv')
    valid_df = spark.read.csv(file_path, schema='geos string').cache()
    valid_df.createOrReplaceTempView("valid")
    sql = "select ST_IsValid(ST_GeomFromText(geos)) from valid"
    calculate(spark, sql)
    calculate_with_timmer('st_isvalid', spark, sql)


@test_log
def run_st_equals(spark):
    file_path = os.path.join(data_path, 'st_equals.csv')
    equals_df = spark.read.csv(file_path, schema="left string, right string").cache()
    equals_df.createOrReplaceTempView("equals")
    sql = "select ST_Equals(ST_GeomFromText(left), ST_GeomFromText(right)) from equals"
    calculate(spark, sql)
    calculate_with_timmer('st_equals', spark, sql)


@test_log
def run_st_touches(spark):
    file_path = os.path.join(data_path, 'st_touches.csv')
    touches_df = spark.read.csv(file_path, schema="left string, right string").cache()
    touches_df.createOrReplaceTempView("touches")
    sql = "select ST_Touches(ST_GeomFromText(left), ST_GeomFromText(right)) from touches"
    calculate(spark, sql)
    calculate_with_timmer('st_touches', spark, sql)


@test_log
def run_st_overlaps(spark):
    file_path = os.path.join(data_path, 'st_overlaps.csv')
    overlaps_df = spark.read.csv(file_path, schema="left string, right string").cache()
    overlaps_df.createOrReplaceTempView("overlaps")
    sql = "select ST_Overlaps(ST_GeomFromText(left), ST_GeomFromText(right)) from overlaps"
    calculate(spark, sql)
    calculate_with_timmer('st_overlaps', spark, sql)


@test_log
def run_st_crosses(spark):
    file_path = os.path.join(data_path, 'st_crosses.csv')
    crosses_df = spark.read.csv(file_path, schema="left string, right string").cache()
    crosses_df.createOrReplaceTempView("crosses")
    sql = "select ST_Crosses(ST_GeomFromText(left), ST_GeomFromText(right)) from crosses"
    calculate(spark, sql)
    calculate_with_timmer('st_crosses', spark, sql)


@test_log
def run_st_issimple(spark):
    file_path = os.path.join(data_path, 'st_issimple.csv')
    simple_df = spark.read.csv(file_path, schema='geos string').cache()
    simple_df.createOrReplaceTempView("simple")
    sql = "select ST_IsSimple(ST_GeomFromText(geos)) from simple"
    calculate(spark, sql)
    calculate_with_timmer('st_issimple', spark, sql)


@test_log
def run_st_geometry_type(spark):
    file_path = os.path.join(data_path, 'st_geometry_type.csv')
    geometry_type_df = spark.read.csv(file_path, schema='geos string').cache()
    geometry_type_df.createOrReplaceTempView("geometry_type")
    sql = "select ST_GeometryType(ST_GeomFromText(geos)) from geometry_type"
    calculate(spark, sql)
    calculate_with_timmer('st_geometry_type', spark, sql)


@test_log
def run_st_make_valid(spark):
    file_path = os.path.join(data_path, 'st_make_valid.csv')
    make_valid_df = spark.read.csv(file_path, schema='geos string').cache()
    make_valid_df.createOrReplaceTempView("make_valid")
    sql = "select ST_AsText(ST_MakeValid(ST_GeomFromText(geos))) from make_valid"
    calculate(spark, sql)
    calculate_with_timmer('st_make_valid', spark, sql)


@test_log
def run_st_simplify_preserve_topology(spark):
    file_path = os.path.join(data_path, 'st_simplify_preserve_topology.csv')
    simplify_preserve_topology_df = spark.read.csv(file_path, schema='geos string').cache()
    simplify_preserve_topology_df.createOrReplaceTempView("simplify_preserve_topology")
    sql = "select ST_AsText(ST_SimplifyPreserveTopology(ST_GeomFromText(geos), 10)) from simplify_preserve_topology"
    calculate(spark, sql)
    calculate_with_timmer('st_simplify_preserve_topology', spark, sql)


@test_log
def run_st_polygon_from_envelope(spark):
    file_path = os.path.join(data_path, 'st_polygon_from_envelope.csv')
    polygon_from_envelope_df = spark.read.csv(file_path,
                                              schema="min_x string, min_y string, max_x string, max_y string").cache()
    polygon_from_envelope_df.createOrReplaceTempView('polygon_from_envelope')
    sql = "select ST_AsText(ST_PolygonFromEnvelope(min_x, min_y, max_x, max_y)) from polygon_from_envelope"
    calculate(spark, sql)
    calculate_with_timmer('st_polygon_from_envelope', spark, sql)


@test_log
def run_st_contains(spark):
    file_path = os.path.join(data_path, 'st_contains.csv')
    contains_df = spark.read.csv(file_path, schema="left string, right string").cache()
    contains_df.createOrReplaceTempView("contains")
    sql = "select ST_Contains(ST_GeomFromText(left), ST_GeomFromText(right)) from contains"
    calculate(spark, sql)
    calculate_with_timmer('st_contains', spark, sql)


@test_log
def run_st_intersects(spark):
    file_path = os.path.join(data_path, 'st_intersects.csv')
    intersects_df = spark.read.csv(file_path, schema="left string, right string").cache()
    intersects_df.createOrReplaceTempView("intersects")
    sql = "select ST_Intersects(ST_GeomFromText(left), ST_GeomFromText(right)) from intersects"
    calculate(spark, sql)
    calculate_with_timmer('st_intersects', spark, sql)


@test_log
def run_st_within(spark):
    file_path = os.path.join(data_path, 'st_within.csv')
    within_df = spark.read.csv(file_path, schema="left string, right string").cache()
    within_df.createOrReplaceTempView("within")
    sql = "select ST_Within(ST_GeomFromText(left), ST_GeomFromText(right)) from within"
    calculate(spark, sql)
    calculate_with_timmer('st_within', spark, sql)


@test_log
def run_st_distance(spark):
    file_path = os.path.join(data_path, 'st_distance.csv')
    distance_df = spark.read.csv(file_path, schema="left string, right string").cache()
    distance_df.createOrReplaceTempView("distance")
    sql = "select ST_Distance(ST_GeomFromText(left), ST_GeomFromText(right)) from distance"
    calculate(spark, sql)
    calculate_with_timmer('st_distance', spark, sql)


@test_log
def run_st_area(spark):
    file_path = os.path.join(data_path, 'st_area.csv')
    area_df = spark.read.csv(file_path, schema='geos string').cache()
    area_df.createOrReplaceTempView("area")
    sql = "select ST_Area(ST_GeomFromText(geos)) from area"
    calculate(spark, sql)
    calculate_with_timmer('st_area', spark, sql)


@test_log
def run_st_centroid(spark):
    file_path = os.path.join(data_path, 'st_centroid.csv')
    centroid_df = spark.read.csv(file_path, schema='geos string').cache()
    centroid_df.createOrReplaceTempView("centroid")
    sql = "select ST_AsText(ST_Centroid(ST_GeomFromText(geos))) from centroid"
    calculate(spark, sql)
    calculate_with_timmer('st_centroid', spark, sql)


@test_log
def run_st_length(spark):
    file_path = os.path.join(data_path, 'st_length.csv')
    length_df = spark.read.csv(file_path, schema='geos string').cache()
    length_df.createOrReplaceTempView("length")
    sql = "select ST_Length(ST_GeomFromText(geos)) from length"
    calculate(spark, sql)
    calculate_with_timmer('st_length', spark, sql)


@test_log
def run_st_hausdorffdistance(spark):
    file_path = os.path.join(data_path, 'st_hausdorffdistance.csv')
    hausdorff_df = spark.read.csv(file_path, schema="geo1 string, geo2 string").cache()
    hausdorff_df.createOrReplaceTempView("hausdorff")
    sql = "select ST_HausdorffDistance(ST_GeomFromText(geo1),ST_GeomFromText(geo2)) from hausdorff"
    calculate(spark, sql)
    calculate_with_timmer('st_hausdorffdistance', spark, sql)


@test_log
def run_st_convexhull(spark):
    file_path = os.path.join(data_path, 'st_convexhull.csv')
    convexhull_df = spark.read.csv(file_path, schema='geos string').cache()
    convexhull_df.createOrReplaceTempView("convexhull")
    sql = "select ST_AsText(ST_convexhull(ST_GeomFromText(geos))) from convexhull"
    calculate(spark, sql)
    calculate_with_timmer('st_convexhull', spark, sql)


@test_log
def run_st_npoints(spark):
    file_path = os.path.join(data_path, 'st_npoints.csv')
    npoints_df = spark.read.csv(file_path, schema='geos string').cache()
    npoints_df.createOrReplaceTempView("npoints")
    sql = "select ST_NPoints(ST_GeomFromText(geos)) from npoints"
    calculate(spark, sql)
    calculate_with_timmer('st_npoints', spark, sql)


@test_log
def run_st_envelope(spark):
    file_path = os.path.join(data_path, 'st_envelope.csv')
    envelope_df = spark.read.csv(file_path, schema='geos string').cache()
    envelope_df.createOrReplaceTempView("envelope")
    sql = "select ST_AsText(ST_Envelope(ST_GeomFromText(geos))) from envelope"
    calculate(spark, sql)
    calculate_with_timmer('st_envelope', spark, sql)


@test_log
def run_st_buffer(spark):
    file_path = os.path.join(data_path, 'st_buffer.csv')
    buffer_df = spark.read.csv(file_path, schema='geos string').cache()
    buffer_df.createOrReplaceTempView("buffer")
    sql = "select ST_AsText(ST_Buffer(ST_GeomFromText(geos), 1.2)) from buffer"
    calculate(spark, sql)
    calculate_with_timmer('st_buffer', spark, sql)


@test_log
def run_st_union_aggr(spark):
    file_path = os.path.join(data_path, 'st_union_aggr.csv')
    union_aggr_df1 = spark.read.csv(file_path, schema='geos string').cache()
    union_aggr_df1.createOrReplaceTempView("union_aggr1")
    sql = "select ST_GeomFromText(geos) as geos from union_aggr1"
    calculate_union_agg(spark, sql)
    calculate_union_agg_with_timmer('st_union_aggr', spark, sql)


@test_log
def run_st_envelope_aggr(spark):
    file_path = os.path.join(data_path, 'st_envelope_aggr.csv')
    envelope_aggr_df = spark.read.csv(file_path, schema='geos string')
    envelope_aggr_df.createOrReplaceTempView('envelope_aggr')
    sql = "select ST_Envelope_Aggr(ST_GeomFromText(geos)) from envelope_aggr"
    calculate_envelope_agg(spark, sql)
    calculate_envelope_agg_with_timmer('st_envelope_aggr', spark, sql)


@test_log
def run_st_transform(spark):
    file_path = os.path.join(data_path, 'st_transform.csv')
    buffer_df = spark.read.csv(file_path, schema='geos string').cache()
    buffer_df.createOrReplaceTempView("buffer")
    sql = "select ST_AsText(ST_Transform(ST_GeomFromText(geos), 'epsg:4326', 'epsg:3857')) from buffer"
    calculate(spark, sql)
    calculate_with_timmer('st_transform', spark, sql)


@test_log
def run_st_curvetoline(spark):
    file_path = os.path.join(data_path, 'st_pointfromtext.csv')
    buffer_df = spark.read.csv(file_path, schema='geos string').cache()
    buffer_df.createOrReplaceTempView("buffer")
    sql = "select ST_AsText(ST_CurveToLine(ST_GeomFromText(geos))) from buffer"
    calculate(spark, sql)
    calculate_with_timmer('st_pointfromtext', spark, sql)


def parse_args(argv):
    import sys, getopt
    try:
        opts, args = getopt.getopt(argv, "h:p:f:o:", ["path", "function", "output"])
    except getopt.GetoptError:
        print('python test_udf_from_csv.py -p <data path> -f <function name, default all udf functions> -o <output>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(
                'python test_udf_from_csv.py -p <data path> -f <function name, default all udf functions> -o <output>')
            sys.exit()
        elif opt in ("-p", "--path"):
            global data_path
            data_path = arg
        elif opt in ("-f", "--function"):
            global test_name
            test_name = arg.split(',')
        elif opt in ("-o", "--output"):
            global output_path
            output_path = arg
    global to_hdfs, report_file_path
    to_hdfs = is_hdfs(output_path)
    if is_hdfs(output_path):
        global hdfs_url
        output_path = remove_prefix(output_path, "hdfs://")
        hdfs_url = "http://" + output_path.split("/", 1)[0]
        output_path = output_path[output_path.find('/'):]
    report_file_path = os.path.join(output_path, time.strftime("%Y-%m-%d-", time.localtime()) + 'report.txt')


if __name__ == "__main__":
    parse_args(sys.argv[1:])
    if to_hdfs:
        client_hdfs = hdfs.InsecureClient(hdfs_url)
        client_hdfs.makedirs(output_path)
        # create report file in hdfs
        with client_hdfs.write(report_file_path, append=False) as f:
            pass
    else:
        os.makedirs(output_path, exist_ok=True)

    spark_session = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark profile") \
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

    spark_session.stop()
