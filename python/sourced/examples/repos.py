from __future__ import print_function
import os
from sourced.engine import Engine
from pyspark.sql import SparkSession


def main():
    file_path = os.path.dirname(os.path.realpath(__file__))
    repos_path = os.path.join(file_path, '..', '..', '..', 'src', 'test', 'resources', 'siva-files')
    session = SparkSession.builder.appName("test").master('local[*]').getOrCreate()
    engine = Engine(session, repos_path, "siva")
    rows = engine.repositories.select('id').collect()

    repos = [r['id'] for r in rows]

    print("REPOS:")
    for r in repos:
        print(r)


if __name__ == '__main__':
    main()
