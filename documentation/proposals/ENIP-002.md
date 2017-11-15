# Use Borges DB as metadata on Engine instead of config files inside siva files.

| Field | Value |
| --- | --- |
| ENIP | 2 |
| Title | Use Borges DB as metadata on Engine instead of config files inside siva files. |
| Author | Antonio Navarro |
| Status | Rejected |
| Created | 2017-11-15 |
| Updated | 2017-11-21 |
| Target version | 0.X |

## Abstract

In our current pipeline,
the element that is in charge of fetch and organize repositories over root-repositories ([Borges][2]) is already creating a database with a lot of useful metadata about the repositories.
With this ENIP we want to use that metadata to improve Engine performance.

## Rationale

Borges is creating a new row per repository fetched.
The actual schema is:

|Column|Type|Description|
|---|---|---|
|ID|ULID|Unique ULID for a repository |
|CreatedAt|Timestamp||
|UpdatedAt|Timestamp||
|Endpoints|Array[String]|Endpoints is a slice of valid git endpoints to reach this repository. For example, git://host/my/repo.git and https://host/my/repo.git. They are meant to be endpoints of the same exact repository, and not mirrors.|
|FetchStatus|String|Actual status of the repository, it can be "not_found", "fetched", "pending" and "fetching"|
|FetchedAt|Timestamp|FetchedAt is the timestamp of the last time this repository was fetched and archived in our repository storage successfully.|
|FetchedErrorAt|Timestamp|FetchErrorAt is the timestamp of the last fetch error, if any.|
|LastCommitAt|Timestamp|LastCommitAt is the last commit time found in this repository.|
|References|JsonB|References is the current slice of references as present in our repository storage.|
|IsFork|Boolean|IsFork stores if this repository is a fork or not. It can be nil if we don't know.|

The content of the References Json is:

|Column|Type|Description|
|---|---|---|
|Name|String|Name is the full reference name.|
|Hash|Array[Byte]|Hash is the hash of the reference.|
|Init|Array[Byte]|Init is the hash of the init commit reached from this reference.|
|Roots|Array[Array[Byte]]|Roots is a slice of the hashes of all root commits reachable from this reference.|
|Time|Timestamp|Time is the time of the commit this reference points too.|

The JDBC connector returns [json and jsonb types as String][1],
so we should apply a defined function to parse as a StructType to be able to query internal content.

## Specification

Create new method on the Engine that able us to register the "repositories" and "references" tables as views from a JDBC datasource.
We should call that methods something related with Borges because this functionality is heavily sticked to it.
Example: `fromBorgesMetadata(options)`

Because of "references" is one of the columns of the "repositories" table,
we should create a view from a query that applies the `from_json()` function,
and expand the result to make a new table with all the reference elements of the arrays.

As first approach, the actual table schemas will be preserved and data mapped to that schema.
Actually, the schema can be modified if the main columns,
the ones used to join data between tables,
are preserved.

The "repositories" view will filter all repositories that are not in *fetched* status to avoid consistency problems with the existing rooted-repositories.

We also need to check if the names for special references (HEAD and master) are specified at the same way as they are specified into rooted-repositories.

The actual logic to generate the repository id on the Engine will be reused to get that data from Borges table.

## Alternatives

The actual Spark API allows us to create a DataFrame from any JDBC connection.

## Impact

New method on the Engine to use Borges views instead of standard ones.

## References
- [Borges][2]
- [json and jsonb types are returned as String][1]

[1]: https://github.com/apache/spark/blob/0c0ad436ad909364915b910867d08262c62bc95d/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L58
[2]: https://github.com/src-d/borges
