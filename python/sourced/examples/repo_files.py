from __future__ import print_function
import os
from sourced.spark import API as SparkAPI
from pyspark.sql import SparkSession


def main():
    file_path = os.path.dirname(os.path.realpath(__file__))
    repos_path = os.path.join(file_path, '..', '..', '..', 'src', 'test', 'resources', 'siva-files')
    session = SparkSession.builder.appName("test").master('local[*]').getOrCreate()
    api = SparkAPI(session, repos_path)
    rows = api.repositories.references.head_ref.commits.first_reference_commit\
        .files.select('path').collect()

    files = [r['path'] for r in rows]

    print("FILES:")
    for f in files:
        print(f)


if __name__ == '__main__':
    main()
