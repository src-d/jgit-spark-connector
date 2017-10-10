from __future__ import print_function
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

    :param session: spark session to use
    :type session: pyspark.sql.SparkSession
    :param repos_path: path to the folder where siva files are stored
    :type repos_path: str
    """

    def __init__(self, session, repos_path):
        self.session = session
        self.__jsparkSession = session._jsparkSession
        self.session.conf.set('spark.tech.sourced.api.repositories.path', repos_path)
        self.__jvm = self.session.sparkContext._gateway.jvm
        java_import(self.__jvm, 'tech.sourced.api.SparkAPI')
        java_import(self.__jvm, 'tech.sourced.api.package$')
        self.__api = self.__jvm.tech.sourced.api.SparkAPI.apply(self.__jsparkSession, repos_path)
        self.__implicits = getattr(getattr(self.__jvm.tech.sourced.api, 'package$'), 'MODULE$')


    @property
    def repositories(self):
        """
        Returns a DataFrame with the data about the repositories found at
        the specified repositories path in the form of siva files.

        >>> repos_df = api.repositories

        :rtype: RepositoriesDataFrame
        """
        return RepositoriesDataFrame(self.__api.getRepositories(),
                                     self.session, self.__implicits)


    def files(self, repository_ids=[], reference_names=[], commit_hashes=[]):
        """
        Retrieves the files of a list of repositories, reference names and commit hashes.
        So the result will be a DataFrame of all the files in the given commits that are
        in the given references that belong to the given repositories.

        NOTE: due to some specifics in the SparkAPI internals, this is way faster than
        using the DataFrame implicit methods to get the files, although this might change
        in the near future.

        >>> files_df = api.repositories.filter("is_fork = false")\\
        >>>     .references.filter("name = 'some ref'")\\
        >>>     .commits.filter("hash = 'somehash'")
        >>> # is way slower than
        >>> files_df_fast = api.files(repo_ids, ref_names, hashes)

        Calling this function with no arguments is the same as:

        >>> api.repositories.references.commits.files

        What makes this method faster than just the example above is the fact of passing
        repository ids to filter by, so it's recommended to do so. You can pass any number
        of elements to filter by (including none).

        :param repository_ids: list of repository ids to filter by (optional)
        :type repository_ids: list of strings
        :param reference_names: list of reference names to filter by (optional)
        :type reference_names: list of strings
        :param commit_hashes: list of hashes to filter by (optional)
        :type commit_hashes: list of strings
        :rtype: FilesDataFrame
        """
        if not isinstance(repository_ids, list):
            raise Exception("repository_ids must be a list")

        if not isinstance(reference_names, list):
            raise Exception("reference_names must be a list")

        if not isinstance(commit_hashes, list):
            raise Exception("commit_hashes must be a list")

        return FilesDataFrame(self.__api.getFiles(repository_ids,
                                                  reference_names,
                                                  commit_hashes),
                              self.session,
                              self.__implicits)


def _custom_df_instance(func):
    """
    Wraps the resultant DataFrame of the method call with the class of self.

    >>> @_custom_df_instance
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

    :param jdf: Java DataFrame
    :type jdf: py4j.java_gateway.JavaObject
    :param session: Spark Session to use
    :type session: pyspark.sql.SparkSession
    :param implicits: Implicits object from Scala
    :type implicits: py4j.java_gateway.JavaObject
    """

    def __init__(self, jdf, session, implicits):
        DataFrame.__init__(self, jdf, session)
        self._session = session
        self._implicits = implicits


    @property
    def _api_dataframe(self):
        return self._implicits.ApiDataFrame(self._jdf)


    @_custom_df_instance
    def checkpoint(self, *args, **kwargs):
        return DataFrame.checkpoint(self, *args, **kwargs)


    @_custom_df_instance
    def withWatermark(self, *args, **kwargs):
        return DataFrame.withWatermark(self, *args, **kwargs)


    @_custom_df_instance
    def hint(self, *args, **kwargs):
        return DataFrame.hint(self, *args, **kwargs)


    @_custom_df_instance
    def limit(self, *args, **kwargs):
        return DataFrame.limit(self, *args, **kwargs)


    @_custom_df_instance
    def coalesce(self, *args, **kwargs):
        return DataFrame.coalesce(self, *args, **kwargs)


    @_custom_df_instance
    def repartition(self, *args, **kwargs):
        return DataFrame.repartition(self, *args, **kwargs)


    @_custom_df_instance
    def distinct(self):
        return DataFrame.distinct(self)


    @_custom_df_instance
    def sample(self, *args, **kwargs):
        return DataFrame.sample(self, *args, **kwargs)


    @_custom_df_instance
    def sampleBy(self, *args, **kwargs):
        return DataFrame.sampleBy(self, *args, **kwargs)


    def randomSplit(self, *args, **kwargs):
        df_list = DataFrame.randomSplit(self, *args, **kwargs)
        if self.__class__ != SourcedDataFrame and isinstance(self, SourcedDataFrame):
            return [self.__class__(df._jdf, self._session, self._implicits) for df in df_list]
        return df_list


    @_custom_df_instance
    def alias(self, *args, **kwargs):
        return DataFrame.alias(self, *args, **kwargs)


    @_custom_df_instance
    def crossJoin(self, other):
        return DataFrame.crossJoin(self, other)


    @_custom_df_instance
    def join(self, *args, **kwargs):
        return DataFrame.join(self, *args, **kwargs)


    @_custom_df_instance
    def sortWithinPartitions(self, *args, **kwargs):
        return DataFrame.sortWithinPartitions(self, *args, **kwargs)


    @_custom_df_instance
    def sort(self, *args, **kwargs):
        return DataFrame.sort(self, *args, **kwargs)


    orderBy = sort


    @_custom_df_instance
    def describe(self, *args, **kwargs):
        return DataFrame.describe(self, *args, **kwargs)


    @_custom_df_instance
    def summary(self, *args, **kwargs):
        return DataFrame.summary(self, *args, **kwargs)


    @_custom_df_instance
    def select(self, *args, **kwargs):
        return DataFrame.select(self, *args, **kwargs)


    @_custom_df_instance
    def selectExpr(self, *args, **kwargs):
        return DataFrame.selectExpr(self, *args, **kwargs)


    @_custom_df_instance
    def filter(self, *args, **kwargs):
        return DataFrame.filter(self, *args, **kwargs)


    @_custom_df_instance
    def union(self, *args, **kwargs):
        return DataFrame.union(self, *args, **kwargs)


    @_custom_df_instance
    def unionByName(self, *args, **kwargs):
        return DataFrame.unionByName(self, *args, **kwargs)


    @_custom_df_instance
    def intersect(self, *args, **kwargs):
        return DataFrame.intersect(self, *args, **kwargs)


    @_custom_df_instance
    def subtract(self, *args, **kwargs):
        return DataFrame.subtract(self, *args, **kwargs)


    @_custom_df_instance
    def dropDuplicates(self, *args, **kwargs):
        return DataFrame.dropDuplicates(self, *args, **kwargs)


    @_custom_df_instance
    def dropna(self, *args, **kwargs):
        return DataFrame.dropna(self, *args, **kwargs)


    @_custom_df_instance
    def fillna(self, *args, **kwargs):
        return DataFrame.fillna(self, *args, **kwargs)


    @_custom_df_instance
    def replace(self, *args, **kwargs):
        return DataFrame.replace(self, *args, **kwargs)


    @_custom_df_instance
    def crosstab(self, *args, **kwargs):
        return DataFrame.crosstab(self, *args, **kwargs)


    @_custom_df_instance
    def freqItems(self, *args, **kwargs):
        return DataFrame.freqItems(self, *args, **kwargs)


    @_custom_df_instance
    def withColumn(self, *args, **kwargs):
        return DataFrame.withColumn(self, *args, **kwargs)


    @_custom_df_instance
    def withColumnRenamed(self, *args, **kwargs):
        return DataFrame.withColumnRenamed(self, *args, **kwargs)


    @_custom_df_instance
    def drop(self, *args, **kwargs):
        return DataFrame.drop(self, *args, **kwargs)


    @_custom_df_instance
    def toDF(self, *args, **kwargs):
        return DataFrame.toDF(self, *args, **kwargs)


    drop_duplicates = dropDuplicates

    where = filter


class RepositoriesDataFrame(SourcedDataFrame):
    """
    DataFrame containing repositories.
    This class should not be instantiated directly, please get your RepositoriesDataFrame using the
    provided methods.

    :param jdf: Java DataFrame
    :type jdf: py4j.java_gateway.JavaObject
    :param session: Spark Session to use
    :type session: pyspark.sql.SparkSession
    :param implicits: Implicits object from Scala
    :type implicits: py4j.java_gateway.JavaObject
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    @property
    def references(self):
        """
        Returns the joined DataFrame of references and repositories.

        >>> refs_df = repos_df.references

        :rtype: ReferencesDataFrame
        """
        return ReferencesDataFrame(self._api_dataframe.getReferences(),
                                   self._session, self._implicits)


class ReferencesDataFrame(SourcedDataFrame):
    """
    DataFrame with references.
    This class should not be instantiated directly, please get your ReferencesDataFrame using the
    provided methods.

    :param jdf: Java DataFrame
    :type jdf: py4j.java_gateway.JavaObject
    :param session: Spark Session to use
    :type session: pyspark.sql.SparkSession
    :param implicits: Implicits object from Scala
    :type implicits: py4j.java_gateway.JavaObject
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    @property
    def head_ref(self):
        """
        Filters the current DataFrame to only contain those rows whose reference is HEAD.

        >>> heads_df = refs_df.head_ref

        :rtype: ReferencesDataFrame
        """
        return self.ref('refs/heads/HEAD')


    @property
    def master_ref(self):
        """
        Filters the current DataFrame to only contain those rows whose reference is master.

        >>> master_df = refs_df.master_ref

        :rtype: ReferencesDataFrame
        """
        return self.ref('refs/heads/master')


    def ref(self, ref):
        """
        Filters the current DataFrame to only contain those rows whose reference is the given
        reference name.

        >>> heads_df = refs_df.ref('refs/heads/HEAD')

        :param ref: Reference to get
        :type ref: str
        :rtype: ReferencesDataFrame
        """
        return ReferencesDataFrame(self.filter(self.name == ref)._jdf,
                                   self._session, self._implicits)


    @property
    def commits(self):
        """
        Returns the current DataFrame joined with the commits DataFrame.

        >>> commits_df = refs_df.commits

        :rtype: CommitsDataFrame
        """
        return CommitsDataFrame(self._api_dataframe.getCommits(), self._session, self._implicits)


    @property
    def files(self):
        """
        Returns this DataFrame joined with the files DataSource.

        >>> files_df = refs_df.files

        :rtype: FilesDataFrame
        """
        return FilesDataFrame(self._api_dataframe.getFiles(), self._session, self._implicits)


class CommitsDataFrame(SourcedDataFrame):
    """
    DataFrame with commits data.
    This class should not be instantiated directly, please get your CommitsDataFrame using the
    provided methods.

    :param jdf: Java DataFrame
    :type jdf: py4j.java_gateway.JavaObject
    :param session: Spark Session to use
    :type session: pyspark.sql.SparkSession
    :param implicits: Implicits object from Scala
    :type implicits: py4j.java_gateway.JavaObject
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    @property
    def first_reference_commit(self):
        """
        Returns a new DataFrame with only the first commit in a reference.
        Without calling this method, commits may appear multiple times in your DataFrame,
        because most of your commits will be shared amongst references. Calling this, your
        DataFrame will only contain the HEAD commit of each reference.

        For the next example, consider we have a master branch with 100 commits and a "foo" branch
        whose parent is the HEAD of master and has two more commits.

        >>> > commits_df.count()
        >>> 102
        >>> > commits_df.first_reference_commit.count()
        >>> 2

        :rtype: CommitsDataFrame
        """
        return CommitsDataFrame(self._api_dataframe.getFirstReferenceCommit(), self._session,
                                self._implicits)


    @property
    def files(self):
        """
        Returns this DataFrame joined with the files DataSource.

        >>> files_df = commits_df.FilesDataFrame

        :rtype: FilesDataFrame
        """
        return FilesDataFrame(self._api_dataframe.getFiles(), self._session, self._implicits)


class FilesDataFrame(SourcedDataFrame):
    """
    DataFrame containing files data.
    This class should not be instantiated directly, please get your FilesDataFrame using the
    provided methods.

    :param jdf: Java DataFrame
    :type jdf: py4j.java_gateway.JavaObject
    :param session: Spark Session to use
    :type session: pyspark.sql.SparkSession
    :param implicits: Implicits object from Scala
    :type implicits: py4j.java_gateway.JavaObject
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    def classify_languages(self):
        """
        Returns a new DataFrame with the language data of any file added to
        its row.

        >>> files_lang_df = files_df.classify_languages

        :rtype: FilesWithLanguageDataFrame
        """
        return FilesWithLanguageDataFrame(self._api_dataframe.classifyLanguages(),
                                          self._session, self._implicits)


    def extract_uasts(self):
        """
        Returns a new DataFrame with the parsed UAST data of any file added to
        its row.

        >>> files_df.extract_uasts

        :rtype: UASTsDataFrame
        """
        return UASTsDataFrame(self._api_dataframe.extractUASTs(),
                              self._session, self._implicits)


class FilesWithLanguageDataFrame(SourcedDataFrame):
    """
    DataFrame containing files and language data.
    This class should not be instantiated directly, please get your FilesWithLanguageDataFrame
    using the provided methods.

    :param jdf: Java DataFrame
    :type jdf: py4j.java_gateway.JavaObject
    :param session: Spark Session to use
    :type session: pyspark.sql.SparkSession
    :param implicits: Implicits object from Scala
    :type implicits: py4j.java_gateway.JavaObject
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)


    def extract_uasts(self):
        """
        Returns a new DataFrame with the parsed UAST data of any file added to
        its row.

        >>> files_lang_df.extract_uasts

        :rtype: UASTsDataFrame
        """
        return UASTsDataFrame(self._api_dataframe.extractUASTs(),
                              self._session, self._implicits)


class UASTsDataFrame(SourcedDataFrame):
    """
    DataFrame containing UAST data.
    This class should not be instantiated directly, please get your UASTsDataFrame using the
    provided methods.

    :param jdf: Java DataFrame
    :type jdf: py4j.java_gateway.JavaObject
    :param session: Spark Session to use
    :type session: pyspark.sql.SparkSession
    :param implicits: Implicits object from Scala
    :type implicits: py4j.java_gateway.JavaObject
    """

    def __init__(self, jdf, session, implicits):
        SourcedDataFrame.__init__(self, jdf, session, implicits)
