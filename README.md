This project demonstrates a difference in the behavior of the
`com.datastax.driver.core.Metadata#getKeyspace` method between versions 2.0.0-rc2 and 2.0.1 of the
DataStax Java driver.

The behavior is explained in this JIRA [post](https://datastax-oss.atlassian.net/browse/JAVA-269).
The tests have been updated so that they all work with version 2.0.1 of the driver!
