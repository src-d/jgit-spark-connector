from __future__ import print_function
import os
from sourced.spark import API as SparkAPI
from pyspark.sql import SparkSession


def main():
    file_path = os.path.dirname(os.path.realpath(__file__))
    repos_path = os.path.join(file_path, '..', '..', '..', 'src', 'test', 'resources', 'siva-files')
    session = SparkSession.builder.appName("test").master('local[*]').getOrCreate()
    api = SparkAPI(session, repos_path)
    refs = api.repositories.filter('id = "github.com/xiyou-linuxer/faq-xiyoulinux"')\
        .references.select('name').collect()

    refs = [r['name'] for r in refs]

    print("REFERENCES:")
    for r in refs:
        print(r)


if __name__ == '__main__':
    main()
