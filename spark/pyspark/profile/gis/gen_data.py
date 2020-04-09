import os
import sys

rows = 0
hdfs_url = ""
data_path = ""
test_name = []
client_hdfs = None


def parse_args(argv):
    import getopt
    try:
        opts, args = getopt.getopt(argv, "hr:u:p:t:", ["rows=", "hdfs url", "path=", "function name"])
    except getopt.GetoptError:
        print('gen_data.py -r <rows> -f <hdfs url> -p <data_path> -f <function name>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('gen_data.py -r <rows> -f <hdfs url> -p <data_path> -f <function name>')
            sys.exit()
        elif opt in ("-r", "--rows"):
            global rows
            rows = int(arg)
        elif opt in ("-p", "--path"):
            global data_path
            data_path = os.path.join(data_path, str(rows))
        elif opt in ("-f", "--function"):
            global test_name
            test_name = arg.split(',')
        elif opt in ("-u", "--url"):
            global hdfs_url
            hdfs_url = arg


import pandas as pd
from hdfs import InsecureClient


def gen_st_point():
    points = [0.1, 0.2]
    df = pd.DataFrame(data=points)
    with client_hdfs.write(os.path.join(data_path, 'st_points.csv'), encoding='utf-8') as writer:
        for i in range(rows):
            df.to_csv(writer)



funcs = {
    'st_point': gen_st_point,
    'st_intersection': gen_st_intersection,
    'st_isvalid': gen_st_isvalid,
    'st_equals': gen_st_equals,
    'st_touches': gen_st_touches,
    'st_overlaps': gen_st_overlaps,
    'st_crosses': gen_st_crosses,
    'st_issimple': gen_st_issimple,
    'st_geometry_type': gen_st_geometry_type,
    'st_make_valid': gen_st_make_valid,
    'st_simplify_preserve_topology': gen_st_simplify_preserve_topology,
    'st_polygon_from_envelope': gen_st_polygon_from_envelope,
    'st_contains': gen_st_contains,
    'st_intersects': gen_st_intersects,
    'st_within': gen_st_within,
    'st_distance': gen_st_distance,
    'st_area': gen_st_area,
    'st_centroid': gen_st_centroid,
    'st_length': gen_st_length,
    'st_hausdorffdistance': gen_st_hausdorffdistance,
    'st_convexhull': gen_st_convexhull,
    'st_npoints': gen_st_npoints,
    'st_envelope': gen_st_envelope,
    'st_buffer': gen_st_buffer,
    'st_union_aggr': gen_st_union_aggr,
    'st_envelope_aggr': gen_st_envelope_aggr,
    'st_transform': gen_st_transform,
    'st_curvetoline': gen_st_curvetoline,
    'st_geomfromgeojson': gen_st_geomfromgeojson,
    'st_pointfromtext': gen_st_pointfromtext,
    'st_polygonfromtext': gen_st_polygonfromtext,
    'st_linestringfromtext': gen_st_linestringfromtext,
    'st_geomfromwkt': gen_st_geomfromwkt,
    'st_geomfromtext': gen_st_geomfromtext,
    'st_astext': gen_st_astext,
}

if __name__ == "__main__":
    parse_args(sys.argv[1:])
    client_hdfs = InsecureClient(hdfs_url)
    test_name = test_name or funcs.keys()
    for name in test_name:
        funcs[name]()
