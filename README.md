This project demonstrates a difference in the behavior of the
`com.datastax.driver.core.Metadata#getKeyspace` method between versions 2.0.0-rc2 and 2.0.1 of the
DataStax Java driver.

The file `TestKeyspaceCreateAndCheck.java` contains several test cases that create keyspaces with
upper- and lower-case names and then check whether they exist by making calls to `getKeyspace` and
by looping through the results of `getKeyspaces`.
