import os
from sourced.engine import Engine
from pyspark.sql import SparkSession

def main():
    file_path = os.path.dirname(os.path.realpath(__file__))
    repos_path = os.path.join(file_path, '..', '..', '..', 'src', 'test', 'resources', 'siva-files')
    session = SparkSession.builder.appName("test").master('local[*]').getOrCreate()
    engine = Engine(session, repos_path)
    engine.repositories.references\
        .filter('name = "refs/heads/develop"')\
        .commits.first_reference_commit.tree_entries.blobs\
        .classify_languages()\
        .filter('lang = "Ruby"')\
        .extract_uasts()\
        .show()


if __name__ == '__main__':
    main()
