package tech.sourced.spark

import tech.sourced.api._

case class DenormalizedBlob
(
    repository: RepositoryDescriptor,
    reference: ResolvedReference,
    commit: Commit,
    file: File,
    blob: Blob
)