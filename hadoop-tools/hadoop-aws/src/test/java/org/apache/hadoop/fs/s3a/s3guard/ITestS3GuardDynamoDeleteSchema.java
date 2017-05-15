package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.Tristate;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_REGION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * CLOUDERA-BUILD. Test to make sure our older code (pre-delete tracking
 * support) works properly with newer DynamoDB schema that includes a
 * is_deleted column.
 *
 * "previous" 5.11.0 code: Does not understand is_deleted column at all:
 *     ignores column.
 * "old" 5.11.1+ code: Ignores any row with is_deleted=true.  Otherwise same as
 *     above.
 * "new" 5.12.0+ code: Has full support for delete tracking (reads and writes
 *    column).
 */
public class ITestS3GuardDynamoDeleteSchema {

  static final String BUCKET = "dummy-bucket";
  static URI fsURI;
  static Configuration conf;

  DynamoDBMetadataStore ms;

  static final String TABLE_NAME = "test-delete-schema";

  // Random constants to build realistic FileStatus'
  private final long accessTime = System.currentTimeMillis();
  private final long modTime = accessTime - 5000;
  static final long BLOCK_SIZE = 32 * 1024 * 1024;
  static final int REPLICATION = 1;
  static final FsPermission PERMISSION = new FsPermission((short)0755);
  static final String OWNER = "bob";
  static final String GROUP = "uncles";

  @BeforeClass
  public static void beforeClass() throws Exception {
    fsURI = new URI("s3a://" + BUCKET + "/");

    // Set table name / region unless already in config
    conf = new Configuration();
    if (conf.get(S3GUARD_DDB_TABLE_NAME_KEY) == null) {
      conf.set(S3GUARD_DDB_TABLE_NAME_KEY, TABLE_NAME);
    }

    // XXX will this break in other regions?
    if (conf.get(S3GUARD_DDB_REGION_KEY) == null) {
      conf.set(S3GUARD_DDB_REGION_KEY, "us-west-2");
    }
  }


  @Before
  public void setup() throws Exception {
    ms = new DynamoDBMetadataStore();
    ms.initialize(conf);
  }

  @After
  public void destroy() throws Exception {
    ms.destroy();
  }

  @Test
  public void testOldCodeOldSchema() throws Exception {
    String dirPrefix = "/old-schema";
    testCodeVsSchema(dirPrefix, false);
  }

  @Test
  public void testOldCodeNewSchema() throws Exception {
    String dirPrefix = "/new-schema";
    testCodeVsSchema(dirPrefix, true);
  }

  private void testCodeVsSchema(String dirPrefix, boolean useNewDelete)
      throws IOException {

    // Create dirs and files
    setUpDeleteTest(dirPrefix);

    // Delete one of the files
    Path toDelete = strToPath(dirPrefix + "/dir-a1/dir-b1/file2");
    if (useNewDelete) {
      ms._delete_with_tombstone(toDelete);
    } else {
      ms.delete(toDelete);
    }

    // Assert get() no longer returns it
    PathMetadata meta = ms.get(toDelete);
    assertNull("File should have been deleted", meta);

    // Assert listChildren() no longer returns it
    DirListingMetadata dirMeta = ms.listChildren(strToPath(dirPrefix +
        "/dir-a1/dir-b1"));

    assertListingsEqual(dirMeta.getListing(),
        dirPrefix + "/dir-a1/dir-b1/file1");
  }

  @Test
  public void testEmptyDirectoryFlag() throws Exception {
    String root = "/empty-directory-flag";
    setUpDeleteTest(root);
    String dir = root + "/dir-a1/dir-b1";

    ms._delete_with_tombstone(strToPath(dir + "/file1"));
    ms._delete_with_tombstone(strToPath(dir + "/file2"));
    PathMetadata meta = ms.get(strToPath(dir), true);
    assertTrue("Recently emptied directory was not reported as possibly empty",
        meta.isEmptyDirectory() != Tristate.FALSE);
  }

  @Test
  public void testInvariantAfterImplicitlyRecreatingParent() throws
      Exception {
    String root = "/invariant-when-recreating-parent";
    String parent = root + "/parent";
    String child = parent + "/child";
    ms.put(new PathMetadata(makeFileStatus(child, 1024)));
    ms._delete_with_tombstone(strToPath(parent));
    ms.delete(strToPath(child));
    ms.put(new PathMetadata(makeFileStatus(child, 1024)));
    assertNotNull("Parent directory was not recreated when child was",
        ms.get(strToPath(parent)));
  }


  /* Helper Functions... Mostly copied from MetadataStoreTestBase */

  private void setUpDeleteTest(String prefix) throws IOException {
    createNewDirs(prefix + "/dir-a1",
        prefix + "/dir-a2",
        prefix + "/dir-a1/dir-b1");
    ms.put(new PathMetadata(makeFileStatus(prefix + "/dir-a1/dir-b1/file1",
        100)));
    ms.put(new PathMetadata(makeFileStatus(prefix + "/dir-a1/dir-b1/file2",
        100)));

    PathMetadata meta = ms.get(strToPath(prefix + "/dir-a1/dir-b1/file2"));
    assertNotNull("Found test file", meta);
  }

  private void assertListingsEqual(Collection<PathMetadata> listing,
      String ...pathStrs) {
    Set<Path> a = new HashSet<>();
    for (PathMetadata meta : listing) {
      a.add(meta.getFileStatus().getPath());
    }

    Set<Path> b = new HashSet<>();
    for (String ps : pathStrs) {
      b.add(strToPath(ps));
    }
    assertEquals("Same set of files", b, a);
  }

  private void createNewDirs(String... dirs)
      throws IOException {
    for (String pathStr : dirs) {
      ms.put(new PathMetadata(makeDirStatus(pathStr)));
    }
  }

  FileStatus basicFileStatus(Path path, int size, boolean isDir,
      long newModTime, long newAccessTime) throws IOException {
    return new FileStatus(size, isDir, REPLICATION, BLOCK_SIZE, newModTime,
        newAccessTime, PERMISSION, OWNER, GROUP, path);
  }

  private FileStatus makeFileStatus(String pathStr, int size) throws
      IOException {
    return makeFileStatus(pathStr, size, modTime, accessTime);
  }

  private FileStatus makeFileStatus(String pathStr, int size, long newModTime,
      long newAccessTime) throws IOException {
    return basicFileStatus(strToPath(pathStr), size, false,
        newModTime, newAccessTime);
  }

  private FileStatus makeDirStatus(String pathStr) throws IOException {
    return basicFileStatus(strToPath(pathStr), 0, true, modTime, accessTime);
  }

  private Path strToPath(String p) {
    final Path path = new Path(p);
    assert path.isAbsolute();
    return path.makeQualified(fsURI, null);
  }
}
