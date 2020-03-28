#!/usr/bin sh
cd /home/shengjh/work/arctern/python/profile/
for i in {0..36} ; do
  # shellcheck disable=SC1068
  py-spy record --native --output ${i} -- python profile_geo.py -f ${i}
done