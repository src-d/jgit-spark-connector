package tech.sourced.api

case class RepositoryDescriptor
(
    initHash: SHA1,
    repositoryID: UUID,
    repositoryURL: Option[String],
    isMainRepository: Boolean
)

case class ResolvedReference
(
    name: String,
    hash: SHA1
)

case class Commit
(
  parents: Array[SHA1],
  message: String,
  author: Signature,
  committer: Signature,
  tree: SHA1
)

case class Signature
(
  Name: String,
  Email: String,
  // FIXME: Use OffsetDateTime
  when: Long
)

case class File
(
path: String
)

case class Blob
(
  hash: SHA1,
  content: Array[Byte]
)