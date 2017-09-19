from sourced.spark import API as SparkAPI
from pyspark.sql import functions as F
from .base import BaseTestCase
from unittest import expectedFailure
from os import path


REPOSITORIES = ['anongit.kde.org/purpose.git',
                'anongit.kde.org/scratch/apol/purpose.git',
                'github.com/mawag/faq-xiyoulinux',
                'github.com/wayneeseguin/sequel',
                'github.com/xiyou-linuxer/faq-xiyoulinux']


REPOSITORY_COMMITS = {'github.com/xiyou-linuxer/faq-xiyoulinux': 927,
                      'github.com/wayneeseguin/sequel': 1212,
                      'github.com/mawag/faq-xiyoulinux': 135,
                      'anongit.kde.org/scratch/apol/purpose.git': 197,
                      'anongit.kde.org/purpose.git': 1973}


class APITestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        file_path = path.dirname(path.realpath(__file__))
        repos_path = path.join(file_path, '..', '..', 'src', 'test', 'resources', 'siva-files')
        self.api = SparkAPI(self.session, repos_path)


    def test_repositories(self):
        df = self.api.repositories
        ids = [row.id for row in df.sort(df.id).select(df.id).collect()]
        self.assertEqual(ids, REPOSITORIES)


    def test_references(self):
        df = self.api.repositories.references
        refs = df.select(df.name).distinct().collect()
        self.assertEquals(len(refs), 44)

    
    def test_references_head(self):
        df = self.api.repositories.references.head_ref
        hashes = [r.hash for r in df.distinct().sort(df.hash).collect()]
        self.assertEqual(hashes, ['202ceb4d3efd2294544583a7d4dc92899aa0181f', 
                                  '2060ee6252a64337c404a4fb44baf374c0bc7f7a', 
                                  'dbfab055c70379219cbcf422f05316fdf4e1aed3', 
                                  'fff7062de8474d10a67d417ccea87ba6f58ca81d', 
                                  'fff7062de8474d10a67d417ccea87ba6f58ca81d'])


    def test_references_master(self):
        df = self.api.repositories.references.master_ref
        hashes = [r.hash for r in df.distinct().sort(df.hash).collect()]
        self.assertEqual(hashes, ['202ceb4d3efd2294544583a7d4dc92899aa0181f', 
                                  '2060ee6252a64337c404a4fb44baf374c0bc7f7a', 
                                  'dbfab055c70379219cbcf422f05316fdf4e1aed3', 
                                  'fff7062de8474d10a67d417ccea87ba6f58ca81d', 
                                  'fff7062de8474d10a67d417ccea87ba6f58ca81d'])


    def test_references_ref(self):
        df = self.api.repositories.references.ref('refs/pull/23/head')
        self.assertEqual(len(df.collect()), 1)


    def test_commits(self):
        df = self.api.repositories.references.commits
        repo_commits = df.groupBy(df.repository_id)\
            .count()\
            .collect()

        self.assertEquals(len(repo_commits), len(REPOSITORIES))
        for repo in repo_commits:
            self.assertEqual(repo['count'], 
                             REPOSITORY_COMMITS[repo.repository_id])


    def test_blobs(self):
        df = self.api.repositories.references.commits.blobs
        self.assertEqual(df.count(), 5275)
        blob = df.sort(df.file_hash).limit(1).first()
        self.assertEqual(blob.file_hash, "0024974e4b56afc8dea0d20e4ca90c1fa4323ce5")
        self.assertEqual(blob.path, 'sequel_core/stress/mem_array_keys.rb')


    @expectedFailure
    def test_classify_languages(self):
        df = self.api.repositories.references.commits.blobs\
            .classify_languages()


    @expectedFailure
    def test_parse_uasts(self):
        df = self.api.repositories.references.commits.blobs\
            .classify_languages().parse_uasts()