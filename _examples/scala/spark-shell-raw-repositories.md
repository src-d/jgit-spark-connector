# Raw repositories usage

In this example, the spark-shell is used to show the usage of source{d} engine with raw git repositories.

## Differences with siva usage

What are the main differences between using the engine with siva files and raw git repositories?

* Raw repositories can have non-remote references, siva files do not.
* Even if you have only one repository, you may have N repositories in the output returned by the engine. That's because different origins are treated as different repositories. In short, you'll have as many repositories as remotes in your repository plus one repository that corresponds to the local repository, which is identified by `file://$PATH_TO_REPOSITORY`. This one will always contain non-remote references and the rest of the repositories will always contain remote references.

**Note:** raw repositories refer to `standard` and `bare` repositories.

## Getting repository references

Launch spark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ spark-shell --packages "tech.sourced:engine:[version]"
```

So we can get the repositories like this and we can see that even if we only have one repository, engine says we have two:

```scala
import tech.sourced.engine._
val engine = Engine(spark, "/path/to/repositories", "standard")
println(engine.getRepositories.count())

// Output:
// 2
```

Getting references:

```scala
print(engine.repositories.references.count())

// Output:
// 4
```

If you want a behavior that's more similar to siva files usage you can filter out non-remote references:

```scala
val references = engine.getRepositories.getReferences
println(references.filter(references("is_remote") === true).count())

// Output:
// 2
```

Alternately, you can use the following shorthand:

```scala
print(engine.getRepositories.getRemoteReferences.count())

// Output:
// 2
```

### Caveats

Note that even if in your repository there's a reference named `refs/remotes/origin/master` it will be converted to a reference named `refs/heads/master` that belongs to the repository identified by your origin remote URL.
