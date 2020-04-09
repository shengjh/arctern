import os
import sys

row_per_batch = 100000
rows = 0
hdfs_url = ""
data_path = ""
test_name = []
client_hdfs = None


def parse_args(argv):
    import getopt
    try:
        opts, args = getopt.getopt(argv, "hr:u:p:f:", ["rows=", "hdfs url", "path=", "function name"])
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
            global row_per_batch
            if rows < row_per_batch:
                row_per_batch = rows
        elif opt in ("-p", "--path"):
            global data_path
            data_path = arg
        elif opt in ("-f", "--function"):
            global test_name
            test_name = arg.split(',')
        elif opt in ("-u", "--url"):
            global hdfs_url
            hdfs_url = arg
    data_path = os.path.join(data_path, str(rows))


import pandas as pd
from hdfs import InsecureClient


def gen_st_point():
    total = rows
    with client_hdfs.write(os.path.join(data_path, 'st_points.csv'), overwrite=True, encoding='utf-8') as writer:
        while True:
            x = [0.1] * row_per_batch
            y = [0.2] * row_per_batch
            df = pd.DataFrame(data={'x': x, 'y': y})
            if total == rows:
                df.to_csv(writer, index=False)
            else:
                df.to_csv(writer, index=False, header=False)
            total -= row_per_batch
            if total <= 0:
                break


def gen_st_intersection():
    total = rows
    with client_hdfs.write(os.path.join(data_path, 'st_intersection.csv'), overwrite=True, encoding='utf-8') as writer:
        while True:
            left = ['POINT(0 0)'] * row_per_batch
            right = ['LINESTRING ( 2 0, 0 2 )'] * row_per_batch
            df = pd.DataFrame(data={'left': left, 'right': right})
            if total == rows:
                df.to_csv(writer, index=False)
            else:
                df.to_csv(writer, index=False, header=False)
            total -= row_per_batch
            if total <= 0:
                break


def gen_st_isvalid():
    total = rows
    with client_hdfs.write(os.path.join(data_path, 'st_isvalid.csv'), overwrite=True, encoding='utf-8') as writer:
        while True:
            geos = ['POINT (30 10)'] * row_per_batch
            df = pd.DataFrame(data={'geos': geos})
            if total == rows:
                df.to_csv(writer, index=False)
            else:
                df.to_csv(writer, index=False, header=False)
            total -= row_per_batch
            if total <= 0:
                break


def gen_st_equals():
    total = rows
    with client_hdfs.write(os.path.join(data_path, 'st_equals.csv'), overwrite=True, encoding='utf-8') as writer:
        while True:
            left = ['LINESTRING(0 0, 10 10)'] * row_per_batch
            right = ['LINESTRING(0 0, 5 5, 10 10)'] * row_per_batch
            df = pd.DataFrame(data={'left': left, 'right': right})
            if total == rows:
                df.to_csv(writer, index=False)
            else:
                df.to_csv(writer, index=False, header=False)
            total -= row_per_batch
            if total <= 0:
                break


def gen_st_touches():
    total = rows
    with client_hdfs.write(os.path.join(data_path, 'st_touches.csv'), overwrite=True, encoding='utf-8') as writer:
        while True:
            left = ['LINESTRING(0 0, 1 1, 0 2)'] * row_per_batch
            right = ['POINT(1 1)'] * row_per_batch
            df = pd.DataFrame(data={'left': left, 'right': right})
            if total == rows:
                df.to_csv(writer, index=False)
            else:
                df.to_csv(writer, index=False, header=False)
            total -= row_per_batch
            if total <= 0:
                break


def gen_st_overlaps():
    total = rows
    with client_hdfs.write(os.path.join(data_path, 'st_overlaps.csv'), overwrite=True, encoding='utf-8') as writer:
        while True:
            left = ['POLYGON((1 1, 4 1, 4 5, 1 5, 1 1))'] * row_per_batch
            right = ['POLYGON((3 2, 6 2, 6 6, 3 6, 3 2))'] * row_per_batch
            df = pd.DataFrame(data={'left': left, 'right': right})
            if total == rows:
                df.to_csv(writer, index=False)
            else:
                df.to_csv(writer, index=False, header=False)
            total -= row_per_batch
            if total <= 0:
                break


def gen_st_crosses():
    total = rows
    with client_hdfs.write(os.path.join(data_path, 'st_crosses.csv'), overwrite=True, encoding='utf-8') as writer:
        while True:
            left = ['MULTIPOINT((1 3), (4 1), (4 3))'] * row_per_batch
            right = ['POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))'] * row_per_batch
            df = pd.DataFrame(data={'left': left, 'right': right})
            if total == rows:
                df.to_csv(writer, index=False)
            else:
                df.to_csv(writer, index=False, header=False)
            total -= row_per_batch
            if total <= 0:
                break


class _OneColDecorator(object):
    def __init__(self, f, line):
        self._function_name = f.__name__
        self._line = line
        self._hdfs_file = os.path.join(data_path, f.__name__ + '.csv')

    def __call__(self):
        print(rows)
        total = rows
        with client_hdfs.write(self._hdfs_file, overwrite=True, encoding='utf-8') as writer:
            while True:
                geos = [self._line] * row_per_batch
                df = pd.DataFrame(data={'geos': geos})
                if total == rows:
                    df.to_csv(writer, index=False)
                else:
                    df.to_csv(writer, index=False, header=False)
                total -= row_per_batch
                if total <= 0:
                    break


def OneColDecorator(f=None, line=''):
    if f:
        return _OneColDecorator(f)
    else:
        def wrapper(f):
            return _OneColDecorator(f, line)

        return wrapper


@OneColDecorator(line='POLYGON((1 2, 3 4, 5 6, 1 2))')
def gen_st_issimple():
    pass


@OneColDecorator(line='LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)')
def gen_st_geometry_type():
    pass


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
    # 'st_make_valid': gen_st_make_valid,
    # 'st_simplify_preserve_topology': gen_st_simplify_preserve_topology,
    # 'st_polygon_from_envelope': gen_st_polygon_from_envelope,
    # 'st_contains': gen_st_contains,
    # 'st_intersects': gen_st_intersects,
    # 'st_within': gen_st_within,
    # 'st_distance': gen_st_distance,
    # 'st_area': gen_st_area,
    # 'st_centroid': gen_st_centroid,
    # 'st_length': gen_st_length,
    # 'st_hausdorffdistance': gen_st_hausdorffdistance,
    # 'st_convexhull': gen_st_convexhull,
    # 'st_npoints': gen_st_npoints,
    # 'st_envelope': gen_st_envelope,
    # 'st_buffer': gen_st_buffer,
    # 'st_union_aggr': gen_st_union_aggr,
    # 'st_envelope_aggr': gen_st_envelope_aggr,
    # 'st_transform': gen_st_transform,
    # 'st_curvetoline': gen_st_curvetoline,
    # 'st_geomfromgeojson': gen_st_geomfromgeojson,
    # 'st_pointfromtext': gen_st_pointfromtext,
    # 'st_polygonfromtext': gen_st_polygonfromtext,
    # 'st_linestringfromtext': gen_st_linestringfromtext,
    # 'st_geomfromwkt': gen_st_geomfromwkt,
    # 'st_geomfromtext': gen_st_geomfromtext,
    # 'st_astext': gen_st_astext,
}

if __name__ == "__main__":
    parse_args(sys.argv[1:])
    client_hdfs = InsecureClient(hdfs_url)
    test_name = test_name or funcs.keys()
    for name in test_name:
        funcs[name]()
