#!/usr/bin/env python3

raw = "SELECT * FROM pts1 WHERE I == "

for x in range(1, 2501):
    print(raw + str(x) + ";")
