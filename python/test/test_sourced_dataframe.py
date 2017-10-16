from sourced.engine import SourcedDataFrame
from .base import BaseTestCase


class SourcedDataFrameTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        df = self.session.createDataFrame([('Alice', 18), ('Amy', 23), ('Cole', 22), ('Aaron', 25), ('Sue', 52)])
        self.df = SourcedDataFrame(df._jdf, self.session, None)


    def test_filter(self):
        self.assert_names(self.df.filter(self.df[1] % 2 == 0),
                          ['Alice', 'Cole', 'Sue'])

    def test_sort(self):
        self.assert_names(self.df.sort(self.df[1]),
                          ['Alice', 'Cole', 'Amy', 'Aaron', 'Sue'])


    def assert_names(self, df, names):
        result = [r[0] for r in df.select(df[0]).collect()]
        self.assertEqual(result, names)