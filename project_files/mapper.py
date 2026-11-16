#!/usr/bin/env python3
"""Mapper wizyt w parkach.

Na wyjściu emituje klucz (park_id, dzień z entry_time) oraz składowe
średniej: sumę group_size i liczność 1. Średniej nie liczy, bo combiner
musi móc te składowe dalej sumować.
"""

import csv
import sys


def main():
    reader = csv.reader(sys.stdin)
    for row in reader:
        if len(row) != 7:
            continue
        if row[0] == "visit_id":
            continue

        park_id = row[1].strip()
        entry_time = row[3].strip()
        if not park_id or len(entry_time) < 10:
            continue

        try:
            group_size = int(row[5])
        except ValueError:
            continue

        visit_date = entry_time[:10]
        sys.stdout.write(f"{park_id}\t{visit_date}\t{group_size}\t1\n")


if __name__ == "__main__":
    main()
