package org.kiji.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestKeyspaceCreateAndCheck {
  private static final Logger LOG = LoggerFactory.getLogger(TestKeyspaceCreateAndCheck.class);

  private Cluster mCassandraCluster = null;

  private static String KEYSPACE_LOWER = "\"mykeyspace\"";
  private static String KEYSPACE_UPPER = "\"myKeyspace\"";
  private static String TABLE_UPPER = "\"myTable\"";

  /**
   * Utility class to create a keyspace.
   * @param keyspace
   */
  private void createKeyspace(String keyspace) {
    Session session = mCassandraCluster.connect();

    String queryText = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
        " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}";

    session.execute(queryText);
  }

  /** Pretty basic sanity test. */
  @Test
  public void testBasicKeyspaceCreation() throws Exception {
    createKeyspace(KEYSPACE_LOWER);
    Assert.assertNotNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_LOWER));
  }

  /** Check building a table as well. */
  @Test
  public void testBasicKeyspaceAndTableCreation() throws Exception {
    // Create the keyspace.
    createKeyspace(KEYSPACE_UPPER);
    Assert.assertNotNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_UPPER));

    // Now create the table.
    Session session = mCassandraCluster.connect();
    String queryText = String.format(
        "CREATE TABLE %s.%s (mykey text, myvalue text, PRIMARY KEY (mykey));",
        KEYSPACE_UPPER,
        TABLE_UPPER
    );
    session.execute(queryText);

    // Now check that the table exists.
    KeyspaceMetadata keyspaceMetadata = mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_UPPER);
    Assert.assertNotNull(keyspaceMetadata);

    for (TableMetadata tableMetadata : keyspaceMetadata.getTables()) {
      LOG.info(tableMetadata.getName());
    }
  }

  /** Same sanity test, but with camel case. */
  @Test
  public void testCamelCaseKeyspaceCreation() throws Exception {
    createKeyspace(KEYSPACE_UPPER);
    Assert.assertNotNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_UPPER));
  }

  /**
   * Create an upper-case keyspace and then try to retrieve with the lower-case name.  Fails with
   * both versions of the driver.
   */
  @Test
  public void testKeyspacesCreateUpperCheckLower() throws Exception {
    createKeyspace(KEYSPACE_UPPER);
    Assert.assertNotNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_UPPER));
    Assert.assertNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_LOWER));
  }

  /**
   * Create a lower-case keyspace and then try to retrieve with the upper-case name.  Fails with
   * both versions of the driver.
   */
  @Test
  public void testKeyspacesCreateLowerCheckUpper() throws Exception {
    createKeyspace(KEYSPACE_LOWER);
    Assert.assertNotNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_LOWER));
    Assert.assertNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_UPPER));
  }

  /** A little bit more worrying! */
  @Test
  public void testKeyspacesDistinct() throws Exception {
    createKeyspace(KEYSPACE_LOWER);
    createKeyspace(KEYSPACE_UPPER);
    Assert.assertNotEquals(
        mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_LOWER),
        mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_UPPER)
    );
  }

  /** Check what actually gets created. */
  @Test
  public void testKeyspacesActuallyCreated() throws Exception {
    createKeyspace(KEYSPACE_LOWER);
    Assert.assertNotNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_LOWER));
    createKeyspace(KEYSPACE_UPPER);
    Assert.assertNotNull(mCassandraCluster.getMetadata().getKeyspace(KEYSPACE_UPPER));

  }

  /**
   * Ensure that the EmbeddedCassandraService for unit tests is running.  If it is not, then start it.
   */
  @Before
  public void startEmbeddedCassandraServiceIfNotRunningAndOpenSession() throws Exception {
    try {
      // Use a custom YAML file that specifies different ports from normal for RPC and thrift.
      //File yamlFile = new File(getClass().getResource("/cassandra.yaml").getFile());
      File yamlFile = new File(TestKeyspaceCreateAndCheck.class.getResource("/cassandra.yaml").getFile());

      assert (yamlFile.exists());
      System.setProperty("cassandra.config", "file:" + yamlFile.getAbsolutePath());
      System.setProperty("cassandra-foreground", "true");

      // Make sure that all of the directories for the commit log, data, and caches are empty.
      // Thank goodness there are methods to get this information (versus parsing the YAML directly).
      ArrayList<String> directoriesToDelete = new ArrayList<String>(Arrays.asList(
          DatabaseDescriptor.getAllDataFileLocations()
      ));
      directoriesToDelete.add(DatabaseDescriptor.getCommitLogLocation());
      directoriesToDelete.add(DatabaseDescriptor.getSavedCachesLocation());
      for (String dirName : directoriesToDelete) {
        FileUtils.deleteDirectory(new File(dirName));
      }
      EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService();
      embeddedCassandraService.start();

    } catch (IOException ioe) {
      throw new IOException("Cannot start embedded C* service!");
    }

    // Use different port from normal here to avoid conflicts with any locally-running C* cluster.
    // Port settings are controlled in "cassandra.yaml" in test resources.
    String hostIp = "127.0.0.1";
    int port = 9043;
    mCassandraCluster = Cluster.builder().addContactPoints(hostIp).withPort(port).build();
  }

  @After
  public void shutdown() {
    mCassandraCluster.close();
  }

}
