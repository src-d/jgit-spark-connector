| Field | Value |
| --- | --- |
| ENIP | 4 |
| Title | Get only first reference commit by default |
| Author | Miguel Molina |
| Status | Accepted |
| Created | 2018-01-10 |
| Updated | 2018-01-10 |
| Target version | `0.4.x` |

## Abstract

The purpose of this proposal is to make the default behavior of the engine to only get the first reference commit (aka the current state of that reference) and only get all reference commits when explicitly asked using a method `getAllReferenceCommits`.

## Rationale

The rationale behind this is that most of the time what you want is `getFirstReferenceCommit`, which is the obvious thing a person would expect engine to do. If you want the more detailed output provided by `getAllReferenceCommits` you are opting in into this, which you already know will severely harm the performance of the job you're running.

So, this as a default that makes more sense for the following reasons:

* More obvious behavior for newcomers.
* Default that does not severely impact performance.
* Previous effect can still be achieved.

## Specification

While this may seem like a very simple and easy issue, it is not as simple once one takes a deep look at how the engine queries are built. Right now, `getFirstReferenceCommit` adds a simple `index = 0` filter, but that's the opt-in behavior. If we want to make it the default, we need to make this change at the iterator level and not adding filters on the query, because then `getAllReferenceCommits` would not be able to remove the filter node.

- `CommitIterator` needs to be changed to just get the first commit if no `index` filter is provided or and get N commits if it is.
- `getAllReferenceCommits` can't provide a single number to match, like `index = 0`, it would need to do the following: `index >= 0`, which would require the `EqualThanOrEqual` filter.
- Implement `EqualThanOrEqual` filter, which requires a complete refactor of the filters, which now work only for equality, providing a list of values that match the given filter instead of returning a function that can be evaluated inside the iterators.


## Alternatives

As an alternative, instead of `index >= 0` one could use `index <> -1`, which would work with the current filters implementation, thus making the implementation of this proposal easier, although hackier.

I would suggest that the first approach is taken, so that we can pave the road to include all possible filter nodes, which would make our iterators more efficient in filtering data without relying on spark to do that.

## Impact

This change breaks compatibility with all prior versions because it changes the output produced by the engine, so it should be released as a `0.4.x` version.

## References

n/a
