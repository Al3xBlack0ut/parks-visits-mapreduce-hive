#!/usr/bin/env python3
"""Combiner: sumuje group_size i liczność w obrębie jednego klucza.

Format wyjścia jest taki sam jak u mappera (park_id, data, suma, liczność),
więc combiner może zadziałać wielokrotnie, a reducer czyta oba źródła tak samo.
"""

import sys


def emit(park_id, visit_date, total_sum, total_count):
    sys.stdout.write(f"{park_id}\t{visit_date}\t{total_sum}\t{total_count}\n")


def main():
    current_key = None
    total_sum = 0
    total_count = 0

    for raw_line in sys.stdin:
        line = raw_line.rstrip("\r\n")
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 4:
            continue

        park_id, visit_date, group_sum, count = parts
        try:
            group_sum = int(group_sum)
            count = int(count)
        except ValueError:
            continue

        key = (park_id, visit_date)
        if current_key is None:
            current_key = key
        elif key != current_key:
            emit(current_key[0], current_key[1], total_sum, total_count)
            current_key = key
            total_sum = 0
            total_count = 0

        total_sum += group_sum
        total_count += count

    if current_key is not None and total_count:
        emit(current_key[0], current_key[1], total_sum, total_count)


if __name__ == "__main__":
    main()
