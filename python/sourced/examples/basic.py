import os
from sourced.engine import Engine
from pyspark.sql import SparkSession

def main():
    file_path = os.path.dirname(os.path.realpath(__file__))
    repos_path = os.path.join(file_path, '..', '..', '..', 'src', 'test', 'resources', 'siva-files')
    session = SparkSession.builder.appName("test").master('local[*]').getOrCreate()
    engine = Engine(session, repos_path, "siva")
    engine.repositories.references.master_ref.commits.show()


if __name__ == '__main__':
    main()
