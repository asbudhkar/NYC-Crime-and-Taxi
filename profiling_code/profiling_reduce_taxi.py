#!/usr/bin/env python
import sys

lines = sys.stdin.readlines()
max_zone = -sys.maxint
for line in lines:
	(key, val) = line.strip().split("\t")
	x = key.split(',')
	drop_zone = key[3]
	drop_zone = max(int(drop_zone), max_zone)

print "%s\t%s" % (max_zone, 1)