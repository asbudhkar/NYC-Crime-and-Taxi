#!/usr/bin/env python
import sys 

for line in sys.stdin:
    val = line.strip()
    data = val.split(',')

    pickup_dt = data[1]
    drop_dt   = data[2]
    pickup_zone = data[22]
    drop_zone = data[23]

    if(pickup_dt !='' and drop_dt !='' and pickup_zone != '' and drop_zone != '' ):
        key =  pickup_dt+',' + drop_dt + ',' + pickup_zone + ',' + drop_zone
        print "%s\t%s" % (key, pickup_zone)
        print "%s\t%s" % (key, drop_zone)
