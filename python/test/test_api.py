from sourced.spark import API as SparkAPI
from .base import BaseTestCase
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
        df = self.api.repositories.references.ref('refs/heads/develop')
        self.assertEqual(len(df.collect()), 2)


    def test_commits(self):
        df = self.api.repositories.references.commits
        repo_commits = df.groupBy(df.repository_id)\
            .count()\
            .collect()

        self.assertEqual(len(repo_commits), len(REPOSITORIES))
        for repo in repo_commits:
            self.assertEqual(repo['count'], 
                             REPOSITORY_COMMITS[repo.repository_id])


    def test_commits_first(self):
        df = self.api.repositories.references.filter("name not like 'refs/tags/%'")
        repo_refs = df.groupBy(df.repository_id).count().collect()
        repos = {}
        for repo in repo_refs:
            repos[repo["repository_id"]] = repo["count"]

        df = self.api.repositories.references.commits.first_reference_commit
        repo_commits = df.groupBy(df.repository_id) \
            .count() \
            .collect()

        self.assertEqual(len(repo_commits), len(REPOSITORIES))
        for repo in repo_commits:
            self.assertEqual(repo['count'], repos[repo["repository_id"]])


    def test_files(self):
        df = self.api.repositories.references.commits.files
        self.assertEqual(df.count(), 91944)
        file = df.sort(df.file_hash).limit(1).first()
        self.assertEqual(file.file_hash, "0020a823b6e5b06c9adb7def76ccd7ed098a06b8")
        self.assertEqual(file.path, 'spec/database_spec.rb')


    def test_files_from_refs(self):
        df = self.api.repositories.references.files
        self.assertEqual(df.count(), 91944)
        file = df.sort(df.file_hash).limit(1).first()
        self.assertEqual(file.file_hash, "0020a823b6e5b06c9adb7def76ccd7ed098a06b8")
        self.assertEqual(file.path, 'spec/database_spec.rb')


    def test_classify_languages(self):
        df = self.api.repositories.references.commits.files
        row = df.sort(df.file_hash).limit(1).classify_languages().first()
        self.assertEqual(row.file_hash, "0020a823b6e5b06c9adb7def76ccd7ed098a06b8")
        self.assertEqual(row.path, 'spec/database_spec.rb')
        self.assertEqual(row.lang, "Ruby")


    def test_extract_uasts(self):
        df = self.api.repositories.references.commits.files
        row = df.sort(df.file_hash).limit(1).classify_languages()\
            .extract_uasts().first()
        self.assertEqual(row.file_hash, "0020a823b6e5b06c9adb7def76ccd7ed098a06b8")
        self.assertEqual(row.path, 'spec/database_spec.rb')
        self.assertEqual(row.lang, "Ruby")
        self.assertEqual(row.uast, b"")

        df = self.api.repositories.references.commits.files
        row = df.sort(df.file_hash).limit(1).extract_uasts().first()
        self.assertEqual(row.file_hash, "0020a823b6e5b06c9adb7def76ccd7ed098a06b8")
        self.assertEqual(row.path, 'spec/database_spec.rb')
        self.assertEqual(row.uast, b"")


    def test_api_files(self):
        rows = self.api.repositories.references.head_ref.commits.sort('hash').limit(10).collect()
        repos = []
        hashes = []
        for row in rows:
            repos.append(row['repository_id'])
            hashes.append(row['hash'])

        self.assertEqual(self.api.files(repos, ["refs/heads/HEAD"], hashes).count(), 655)


    def test_api_files_repository(self):
        files = self.api.files(repository_ids=['github.com/xiyou-linuxer/faq-xiyoulinux'])
        self.assertEqual(files.count(), 2421)


    def test_api_files_reference(self):
        files = self.api.files(reference_names=['refs/heads/develop'])
        self.assertEqual(files.count(), 425)


    def test_api_files_hash(self):
        files = self.api.files(commit_hashes=['fff7062de8474d10a67d417ccea87ba6f58ca81d'])
        self.assertEqual(files.count(), 2)
