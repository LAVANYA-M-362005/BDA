#!/usr/bin/env python3
import json
import sys


for line_number, line in enumerate(sys.stdin, start=1):
    try:
        transaction = json.loads(line)
        product = str(transaction["product_name"]).strip()
        units = int(transaction["units_sold"])
        if not product or units < 0 or "\t" in product or "\n" in product:
            raise ValueError("invalid product or quantity")
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        print(
            f"Invalid HDFS transaction on input line {line_number}: {error}",
            file=sys.stderr,
        )
        raise SystemExit(1) from error

    print(f"{product}\t{units}")
