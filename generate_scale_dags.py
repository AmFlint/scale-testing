#!/usr/bin/env python3
"""
Generate scale test DAG files for DAG Processor load testing.

Usage:
    python generate_scale_dags.py <num_files> [--dags-per-file N] [--tasks-per-dag N] [--output-dir DIR]

Examples:
    python generate_scale_dags.py 5000
    python generate_scale_dags.py 1000 --dags-per-file 10 --output-dir ./dags
"""

import argparse
import os
import sys
from pathlib import Path

DAG_TEMPLATE = '''\
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["scale-test"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    workers = [EmptyOperator(task_id=f"task_{{i}}") for i in range({num_workers})]
    start >> workers >> end
'''


def generate_dag_file(path: Path, dag_ids: list[str], tasks_per_dag: int) -> None:
    num_workers = tasks_per_dag - 2  # subtract start + end
    lines = [
        "from airflow import DAG",
        "from airflow.operators.empty import EmptyOperator",
        "from datetime import datetime",
        "",
    ]
    for dag_id in dag_ids:
        lines += [
            f'with DAG(',
            f'    dag_id="{dag_id}",',
            f'    start_date=datetime(2024, 1, 1),',
            f'    schedule=None,',
            f'    catchup=False,',
            f'    tags=["scale-test"],',
            f') as dag:',
            f'    start = EmptyOperator(task_id="start")',
            f'    end = EmptyOperator(task_id="end")',
            f'    workers = [EmptyOperator(task_id=f"task_{{i}}") for i in range({num_workers})]',
            f'    start >> workers >> end',
            '',
        ]
    path.write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description="Generate scale test DAG files")
    parser.add_argument("num_files", type=int, help="Number of DAG files to generate")
    parser.add_argument("--dags-per-file", type=int, default=1,
                        help="Number of DAGs per file (default: 1)")
    parser.add_argument("--tasks-per-dag", type=int, default=100,
                        help="Number of tasks per DAG including start+end (default: 100)")
    parser.add_argument("--output-dir", type=str, default="./dags",
                        help="Output directory (default: ./dags)")
    args = parser.parse_args()

    if args.tasks_per_dag < 3:
        print("ERROR: --tasks-per-dag must be at least 3 (start + 1 worker + end)")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    total_dags = args.num_files * args.dags_per_file
    dag_index = 0

    print(f"Generating {args.num_files} files × {args.dags_per_file} DAGs = {total_dags} total DAGs")
    print(f"Each DAG has {args.tasks_per_dag} tasks (start + {args.tasks_per_dag - 2} workers + end)")
    print(f"Output: {output_dir.resolve()}")
    print()

    for file_index in range(args.num_files):
        dag_ids = []
        for _ in range(args.dags_per_file):
            dag_ids.append(f"scale_test_antoine_{dag_index:05d}")
            dag_index += 1

        filename = output_dir / f"scale_test_antoine_{file_index:05d}.py"
        generate_dag_file(filename, dag_ids, args.tasks_per_dag)

        if (file_index + 1) % 500 == 0 or file_index == args.num_files - 1:
            print(f"  [{file_index + 1:>5}/{args.num_files}] files written...")

    print(f"\nDone. {args.num_files} files written to {output_dir.resolve()}")
    print(f"Total DAGs: {total_dags}  |  Total tasks: {total_dags * args.tasks_per_dag:,}")


if __name__ == "__main__":
    main()