This project demonstrates a difference in the behavior of the
`com.datastax.driver.core.Metadata#getKeyspace` method between versions 2.0.0-rc2 and 2.0.1 of the
DataStax Java driver.

The file `TestKeyspaceCreateAndCheck.java` contains several test cases:

- A sanity test that create a keyspace (all lower-case) and checks that it exists
- A test that does the same, but with a camel case keyspace
- A test that uses two keyspaces whose names are identical except for their use of mixed cases

In each test case, we create a keyspace and then use the `getKeyspace` method to check whether it
works.
