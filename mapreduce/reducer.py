#!/usr/bin/env python3
import csv
import sys


totals = {}
for line_number, line in enumerate(sys.stdin, start=1):
    try:
        product, units = line.rstrip("\n").rsplit("\t", 1)
        totals[product] = totals.get(product, 0) + int(units)
    except (ValueError, IndexError) as error:
        print(
            "Invalid mapper output on line {}: {}".format(line_number, error),
            file=sys.stderr,
        )
        raise SystemExit(1) from error

writer = csv.writer(sys.stdout, lineterminator="\n")
writer.writerow(("rank", "product_name", "units_sold"))
ranked = sorted(totals.items(), key=lambda item: (-item[1], item[0]))[:10]
for rank, (product, units) in enumerate(ranked, start=1):
    writer.writerow((rank, product, units))
