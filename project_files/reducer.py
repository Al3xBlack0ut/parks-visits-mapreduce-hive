#!/usr/bin/env python3
"""Reducer: z sumy group_size i liczności wylicza statystyki dnia w parku.

Kolumna sum_group_size jest dodatkowym atrybutem. Hive liczy z niej średnią
po wszystkich wizytach, bez uśredniania średnich dziennych.
"""

import sys


def format_avg(total_sum, total_count):
    text = f"{total_sum / total_count:.6f}".rstrip("0").rstrip(".")
    return text or "0"


def emit(park_id, visit_date, total_sum, total_count):
    avg = format_avg(total_sum, total_count)
    sys.stdout.write(
        f"{park_id}\t{visit_date}\t{total_count}\t{avg}\t{total_sum}\n"
    )


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
