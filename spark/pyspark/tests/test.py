from arctern_pyspark.geoseries import GeoSeries as spark_geos
from pandas import Series as pds
from arctern.geoseries import GeoSeries as python_geos

from arctern._wrapper_func import ST_GeomFromText
from arctern_pyspark._wrapper_func import ST_GeomFromText
from pandas.testing import assert_series_equal
import numpy as np
import pandas as pd


def make_point(x, y):
    return "POINT (%s %s)" % (x, y)


def expected_series():
    return spark_geos.geom_from_text([make_point(x, x) for x in range(5)])

def dic(param):
    if param == 'wkt':
        return {x: make_point(x, x) for x in range(5)}
    return {x: ST_GeomFromText(make_point(x, x))[0] for x in range(5)}

def assert_is_geoseries(s):
    assert isinstance(s, spark_geos)

def test_from_dict(dic, expected_series):
    s = spark_geos(dic)
    assert_is_geoseries(s)
    print(s)
    print(type(expected_series))
    assert s == expected_series

def test_from_with_na_data():
    s = spark_geos(['Point (1 2)', None, np.nan])
    assert_is_geoseries(s)
    print(s)
    print(type(s))
    assert len(s) == 3
    assert s.hasnans()
    print(s.isna())
    assert s[1] is None
    assert s[2] is None

# pandas_s = python_geos(['Point (1 2)', None, np.nan])
# pandas_s_take = pandas_s.take([0, 2])
# print(type(pandas_s_take))
#
koa_s = spark_geos(['Point (1 2)', None, np.nan], crs="epsg:3854")
test_wkt = koa_s.to_wkt()
test_s = spark_geos(make_point(1, 1))
print(type(test_s[0]))
test_fillna = koa_s.fillna(test_s[0])
print(test_fillna)
# print(type(koa_s))
# koa_s_take = koa_s.take([0, 2]).sort_index()
# print(type(koa_s_take))
# koa_s_buffer = koa_s.buffer(1.2)
# print(type(koa_s_buffer))
# print(koa_s_buffer.crs)
# print(koa_s.crs)
# test_from_dict(dic(param="wkt"), expected_series())
# test_from_with_na_data()

def test_missing_values():
    s = spark_geos([make_point(1, 2), None])
    assert s[1] is None
    assert s.isna().tolist() == [False, True]
    assert s.notna().tolist() == [True, False]
    assert not s.dropna().isna().any()

    s1 = s.fillna(make_point(1, 1))
    s1 = s1.to_wkt()
    assert s1[0] == "POINT (1 2)"
    assert s1[1] == "POINT (1 1)"

    # fillna with method
    s1 = s.fillna(method='ffill')
    assert s1[0] == s1[1]

    s1 = s[::-1].fillna(method="bfill")
    assert s1[0] == s1[1]

    # set item with na value
    s[0] = np.nan
    assert s[0] is None

    s[0] = pd.NA
    assert s[0] is None

# test_missing_values()