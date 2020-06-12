/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.oss;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.util.ConfigurationUtils;

import com.aliyun.oss.ServiceException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for the {@link OSSUnderFileSystem}.
 */
public class OSSUnderFileSystemTest {

  private UnderFileSystem mUfs;
  private String mPath;

  @Nullable
  private static final String ENDPOINT = System.getenv(PropertyKey.OSS_ENDPOINT_KEY.getName());

  @Nullable
  private static final String BUCKET = System.getenv("fs.oss.bucket");

  @Nullable
  private static final String ACCESS_KEY = System.getenv(PropertyKey.OSS_ACCESS_KEY.getName());

  @Nullable
  private static final String SECRET_KEY = System.getenv(PropertyKey.OSS_SECRET_KEY.getName());

  private static boolean credentialsAvailable() {
    return ENDPOINT != null && BUCKET != null && ACCESS_KEY != null && SECRET_KEY != null;
  }

  private void assumeCredentialsAvailable() {
    Assume.assumeTrue("No OSS credentials available in this test's environment",
        credentialsAvailable());
  }

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, ServiceException {
    assumeCredentialsAvailable();
    AlluxioProperties properties = ConfigurationUtils.defaults();
    properties.put(PropertyKey.OSS_ENDPOINT_KEY, ENDPOINT, Source.MOUNT_OPTION);
    properties.put(PropertyKey.OSS_ACCESS_KEY, ACCESS_KEY, Source.MOUNT_OPTION);
    properties.put(PropertyKey.OSS_SECRET_KEY, SECRET_KEY, Source.MOUNT_OPTION);
    mPath = String.format("oss://%s", BUCKET);
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find(
        mPath, new InstancedConfiguration(properties));
    mUfs = factory.create(mPath,
        UnderFileSystemConfiguration.defaults(new InstancedConfiguration(properties)));
    Assert.assertEquals("oss", mUfs.getUnderFSType());
  }

  @Test
  public void testBasicReadAndWrite() throws IOException {
    final String testLine = "Hello Upload!";
    final String testPath = mPath + "/test.txt";
    try {
      try (OutputStream out = mUfs.create(testPath);
           OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
        writer.write(testLine);
      }
      Assert.assertTrue(mUfs.exists(testPath));

      try (InputStream in = mUfs.open(testPath);
           InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
           BufferedReader reader = new BufferedReader(ir)) {
        String line = reader.readLine();
        Assert.assertEquals(testLine, line);
      }
    } finally {
      mUfs.deleteFile(testPath);
    }
  }
}
