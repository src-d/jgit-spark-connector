from py4j.java_gateway import java_import
from pyspark.sql import SparkSession, DataFrame


class API(object):
    """
    This is the entry point to any functionality exposed by the source{d}
    SparkAPI. It contains methods to initialize the analysis on top of source
    code repositories.

    >>> from sourced.spark import API as SparkAPI
    >>> repos_df = SparkAPI(sparkSession, "/path/to/my/repositories").repositories
    >>> repos_df.show()
    """

    def __init__(self, session, repos_path):
        self.session = session
        self.__jsparkSession = session._jsparkSession
        self.session.conf.set('tech.sourced.api.repositories.path', repos_path)
        self.__jvm = self.session.sparkContext._gateway.jvm
        java_import(self.__jvm, 'tech.sourced.api.Implicits')
        self.__implicits = self.__jvm.tech.sourced.api.Implicits


    @property
    def repositories(self):
        """
        Returns a DataFrame with the repositories available at the given path.
        """
        return RepositoriesDataFrame(self.__implicits.getDataSource('repositories',
                                                                    self.__jsparkSession),
                                     self.session, self.__implicits)


def custom_df_instance(func):
    """
    Wraps the resultant DataFrame of the method call with the class of self.

    >>> @custom_df_instance
    >>> def method(self, *args, **kwargs):
    >>>    return ParentClass.method(self, *args, **kwargs)
    """
    def _wrapper(self, *args, **kwargs):
        dataframe = func(self, *args, **kwargs)
        if self.__class__ != SourcedDataFrame\
          and isinstance(self, SourcedDataFrame)\
          and isinstance(dataframe, DataFrame):
            return self.__class__(dataframe._jdf, self._session, self._implicits)
        return dataframe
    return _wrapper


class SourcedDataFrame(DataFrame):
    """
    Custom SparkAPI DataFrame that contains some DataFrame overriden methods and utilities.
    This class should not be used directly, please get your SourcedDataFrames using the
    provided methods.
    """

    def __init__(self, jdf, session, implicits):
        DataFrame.__init__(self, jdf, session)
        self._session = session
        self._implicits = implicits


    @property
    def _api_dataframe(self):
        return self._implicits.ApiDataFrame(self._jdf)


    @custom_df_instance
    def checkpoint(self, *args, **kwargs):
        return DataFrame.checkpoint(self, *args, **kwargs)


    @custom_df_instance
    def withWatermark(self, *args, **kwargs):
        return DataFrame.withWatermark(self, *args, **kwargs)


    @custom_df_instance
    def hint(self, *args, **kwargs):
        return DataFrame.hint(self, *args, **kwargs)


    @custom_df_instance
    def limit(self, *args, **kwargs):
        return DataFrame.limit(self, *args, **kwargs)


    @custom_df_instance
    def coalesce(self, *args, **kwargs):
        return DataFrame.coalesce(self, *args, **kwargs)


    @custom_df_instance
    def repartition(self, *args, **kwargs):
        return DataFrame.repartition(self, *args, **kwargs)


    @custom_df_instance
    def distinct(self):
        return DataFrame.distinct(self)


    @custom_df_instance
    def sample(self, *args, **kwargs):
        return DataFrame.sample(self, *args, **kwargs)


    @custom_df_instance
    def sampleBy(self, *args, **kwargs):
        return DataFrame.sampleBy(self, *args, **kwargs)


    def randomSplit(self, *args, **kwargs):
        df_list = DataFrame.randomSplit(self, *args, **kwargs)
        if self.__class__ != SourcedDataFrame and isinstance(self, SourcedDataFrame):
            return [self.__class__(df._jdf, self._session, self._implicits) for df in df_list]
        return df_list


    @custom_df_instance
    def alias(self, *args, **kwargs):
        return DataFrame.alias(self, *args, **kwargs)


    @custom_df_instance
    def crossJoin(self, other):
        return DataFrame.crossJoin(self, other)


    @custom_df_instance
    def join(self, *args, **kwargs):
        return DataFrame.join(self, *args, **kwargs)


    @custom_df_instance
    def sortWithinPartitions(self, *args, **kwargs):
        return DataFrame.sortWithinPartitions(self, *args, **kwargs)


    @custom_df_instance
    def sort(self, *args, **kwargs):
        return DataFrame.sort(self, *args, **kwargs)


    orderBy = sort


    @custom_df_instance
    def describe(self, *args, **kwargs):
        return DataFrame.describe(self, *args, **kwargs)


    @custom_df_instance
    def summary(self, *args, **kwargs):
        return DataFrame.summary(self, *args, **kwargs)


    @custom_df_instance
    def select(self, *args, **kwargs):
        return DataFrame.select(self, *args, **kwargs)


    @custom_df_instance
    def selectExpr(self, *args, **kwargs):
        return DataFrame.selectExpr(self, *args, **kwargs)


    @custom_df_instance
    def filter(self, *args, **kwargs):
        return DataFrame.filter(self, *args, **kwargs)


    @custom_df_instance
    def union(self, *args, **kwargs):
        return DataFrame.union(self, *args, **kwargs)


    @custom_df_instance
    def unionByName(self, *args, **kwargs):
        return DataFrame.unionByName(self, *args, **kwargs)


    @custom_df_instance
    def intersect(self, *args, **kwargs):
        return DataFrame.intersect(self, *args, **kwargs)


    @custom_df_instance
    def subtract(self, *args, **kwargs):
        return DataFrame.subtract(self, *args, **kwargs)


    @custom_df_instance
    def dropDuplicates(self, *args, **kwargs):
        return DataFrame.dropDuplicates(self, *args, **kwargs)


    @custom_df_instance
    def dropna(self, *args, **kwargs):
        return DataFrame.dropna(self, *args, **kwargs)


    @custom_df_instance
    def fillna(self, *args, **kwargs):
        return DataFrame.fillna(self, *args, **kwargs)


    @custom_df_instance
    def replace(self, *args, **kwargs):
        return DataFrame.replace(self, *args, **kwargs)


    @custom_df_instance
    def crosstab(self, *args, **kwargs):
        return DataFrame.crosstab(self, *args, **kwargs)


    @custom_df_instance
    def freqItems(self, *args, **kwargs):
        return DataFrame.freqItems(self, *args, **kwargs)


    @custom_df_instance
    def withColumn(self, *args, **kwargs):
        return DataFrame.withColumn(self, *args, **kwargs)


    @custom_df_instance
    def withColumnRenamed(self, *args, **kwargs):
        return DataFrame.withColumnRenamed(self, *args, **kwargs)


    @custom_df_instance
    def drop(self, *args, **kwargs):
        return DataFrame.drop(self, *args, **kwargs)


    @custom_df_instance
    def toDF(self, *args, **kwargs):
        return DataFrame.toDF(self, *args, **kwargs)


    drop_duplicates = dropDuplicates

    where = filter


class RepositoriesDataFrame(SourcedDataFrame):
    """
    DataFrame containing repositories.
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    @property
    def references(self):
        """
        Returns the joined DataFrame of references and repositories.
        """
        return ReferencesDataFrame(self._api_dataframe.getReferences(),
                                   self._session, self._implicits)


class ReferencesDataFrame(SourcedDataFrame):
    """
    DataFrame with references.
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    @property
    def head_ref(self):
        """
        Filters the current DataFrame to only contain those rows whose reference is HEAD.
        """
        return self.ref('refs/heads/HEAD')


    @property
    def master_ref(self):
        """
        Filters the current DataFrame to only contain those rows whose reference is master.
        """
        return self.ref('refs/heads/master')


    def ref(self, ref):
        """
        Filters the current DataFrame to only contain those rows whose reference is the given
        reference name.
        """
        return ReferencesDataFrame(self.filter(self.name == ref)._jdf,
                                   self._session, self._implicits)


    @property
    def commits(self):
        """
        Returns the current DataFrame joined with the commits DataFrame.
        """
        return CommitsDataFrame(self._api_dataframe.getCommits(), self._session, self._implicits)


class CommitsDataFrame(SourcedDataFrame):
    """
    DataFrame with commits data.
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    @property
    def blobs(self):
        """
        Returns this DataFrame joined with the blobs DataSource.
        """
        return BlobsDataFrame(self._api_dataframe.getFiles(), self._session, self._implicits)


class BlobsDataFrame(SourcedDataFrame):
    """
    DataFrame containing Blobs data.
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(jdf, session, implicits)

    
    def classify_languages(self):
        """
        Returns a new DataFrame with the language data of any blob added to
        its row.
        """
        raise NotImplementedError("classify_languages not yet implemented")


    def parse_uasts(self):
        """
        Returns a new DataFrame with the parsed UAST data of any blob added to
        its row.
        """
        raise NotImplementedError("parse_uasts not yet implemented")