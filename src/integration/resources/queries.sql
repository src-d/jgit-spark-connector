SELECT * FROM repositories WHERE is_fork=false
SELECT * FROM _references WHERE name='refs/heads/master'
SELECT * FROM commits WHERE hash='fff7062de8474d10a67d417ccea87ba6f58ca81d'
SELECT * FROM files WHERE path='README.md'
SELECT r.*, r2.* FROM repositories r JOIN _references r2 ON r.id = r2.repository_id WHERE r2.name = 'refs/heads/master'