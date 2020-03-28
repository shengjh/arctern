import os

funcs = [
    'test_ST_IsValid',
    'test_ST_PrecisionReduce',
    'test_ST_Intersection',
    'test_ST_Equals',
    'test_ST_Touches',
    'test_ST_Overlaps',
    'test_ST_Crosses',
    'test_ST_IsSimple',
    'test_ST_GeometryType',
    'test_ST_MakeValid',
    'test_ST_SimplifyPreserveTopology',
    'test_ST_Point',
    'test_ST_GeomFromGeoJSON',
    'test_ST_Contains',
    'test_ST_Intersects',
    'test_ST_Within',
    'test_ST_Distance',
    'test_ST_Area',
    'test_ST_Centroid',
    'test_ST_Length',
    'test_ST_HausdorffDistance',
    'test_ST_ConvexHull',
    'test_ST_Transform',
    'test_ST_CurveToLine',
    'test_ST_NPoints',
    'test_ST_Envelope',
    'test_ST_Buffer',
    'test_ST_PolygonFromEnvelope',
    'test_ST_Union_Aggr',
    'test_ST_Envelope_Aggr',
]

if __name__ == "__main__":
    for i in range(len(funcs)):
        output = funcs[i] + ".svg "
        print("Ready generate " + output)
        os.system("py-spy record --native --output " + output + "-- python profile_geo.py -f " + str(i))
        print("Generate " + output + "done")
