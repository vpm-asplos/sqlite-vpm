#!/usr/bin/env python3

for i in range(1,23):
    ff = "./normalq/" + str(i) + ".sql"
    fo = open(ff, "r")
    print(fo.read().strip() + ";" + "\n")
