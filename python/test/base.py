import os
from os.path import realpath, dirname, join
from glob import glob

jars_path = join(dirname(dirname(realpath(__file__))), "jars")
jars = ':'.join(glob(join(jars_path, '*.jar')))
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars %s pyspark-shell' % jars

from pyspark.sql import SparkSession
from unittest import TestCase


class BaseTestCase(TestCase):
    def setUp(self):
        self.session = SparkSession.builder.appName("test").master("local[*]").getOrCreate()