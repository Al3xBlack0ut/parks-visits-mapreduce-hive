import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROJECT_FILES = ROOT / "project_files"
FIXTURE = ROOT / "tests" / "fixtures" / "visits.csv"


def run_script(script_name, input_text):
    result = subprocess.run(
        [sys.executable, str(PROJECT_FILES / script_name)],
        input=input_text,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def sort_by_key(text):
    rows = [line for line in text.splitlines() if line]
    rows.sort(key=lambda line: tuple(line.split("\t")[:2]))
    return "\n".join(rows) + ("\n" if rows else "")


class MapReducePipelineTests(unittest.TestCase):
    def test_mapper_combiner_reducer_aggregate_visits(self):
        visits = FIXTURE.read_text(encoding="utf-8")
        mapped = sort_by_key(run_script("mapper.py", visits))
        combined = sort_by_key(run_script("combiner.py", mapped))
        reduced = run_script("reducer.py", combined)

        self.assertEqual(
            set(reduced.splitlines()),
            {
                "PARK0001\t2024-01-01\t2\t3\t6",
                "PARK0001\t2024-01-02\t1\t1\t1",
                "PARK0002\t2024-01-01\t1\t3\t3",
            },
        )


if __name__ == "__main__":
    unittest.main()
