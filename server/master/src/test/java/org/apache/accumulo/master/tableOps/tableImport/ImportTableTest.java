/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.tableOps.tableImport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImportTableTest {

  @Test
  public void testTabletDir() {
    Master master = EasyMock.createMock(Master.class);
    VolumeManager volumeManager = EasyMock.createMock(VolumeManager.class);
    ImportedTableInfo iti = new ImportedTableInfo();
    iti.tableId = TableId.of("5");

    // Different volumes with different paths
    String[] volumes = {"hdfs://nn1:8020/apps/accumulo1", "hdfs://nn2:8020/applications/accumulo"};
    // This needs to be unique WRT the importtable command
    String tabletDir = "/c-00000001";

    EasyMock.expect(master.getContext()).andReturn(null);
    EasyMock.expect(master.getFileSystem()).andReturn(volumeManager);
    // Choose the 2nd element
    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironmentImpl(iti.tableId, null, null);
    EasyMock.expect(volumeManager.choose(EasyMock.eq(chooserEnv), EasyMock.eq(volumes)))
        .andReturn(volumes[1]);

    EasyMock.replay(master, volumeManager);

    PopulateMetadataTable pmt = new PopulateMetadataTable(iti);
    assertEquals(volumes[1] + "/" + ServerConstants.TABLE_DIR + "/" + iti.tableId + "/" + tabletDir,
        pmt.getClonedTabletDir(master, null, volumes, tabletDir));

    EasyMock.verify(master, volumeManager);
  }

  @Test
  public void testParseExportDir() {
    List<ImportedTableInfo.DirectoryMapping> out;

    // null
    out = ImportTable.parseExportDir(null);
    assertEquals(0, out.size());

    // empty
    out = ImportTable.parseExportDir("");
    assertEquals(0, out.size());

    // single
    out = ImportTable.parseExportDir("hdfs://nn1:8020/apps/import");
    assertEquals(1, out.size());
    assertEquals("hdfs://nn1:8020/apps/import", out.get(0).exportDir);
    assertNull(out.get(0).importDir);

    // multiple
    out = ImportTable.parseExportDir("hdfs://nn1:8020/apps/import,hdfs://nn2:8020/apps/import");
    assertEquals(2, out.size());
    assertEquals("hdfs://nn1:8020/apps/import", out.get(0).exportDir);
    assertNull(out.get(0).importDir);
    assertEquals("hdfs://nn2:8020/apps/import", out.get(1).exportDir);
    assertNull(out.get(1).importDir);
  }

  @Test
  public void testFindExportFile() throws Exception {
    Master master = EasyMock.createMock(Master.class);
    VolumeManager volumeManager = EasyMock.createMock(VolumeManager.class);

    String[] volumes = {"hdfs://nn1:8020/apps/accumulo1", "hdfs://nn2:8020/applications/accumulo",
        "hdfs://nn3:8020/applications/accumulo"};

    EasyMock.expect(master.getFileSystem()).andReturn(volumeManager).times(2);
    EasyMock.expect(volumeManager.exists(EasyMock.eq(new Path(volumes[0], Constants.EXPORT_FILE))))
        .andReturn(Boolean.FALSE);
    EasyMock.expect(volumeManager.exists(EasyMock.eq(new Path(volumes[1], Constants.EXPORT_FILE))))
        .andReturn(Boolean.TRUE);

    ImportTable it = new ImportTable("testUser", "testTable", volumes[0] + "," + volumes[1], null);

    EasyMock.replay(master, volumeManager);

    Path p = it.findExportFile(master);

    assertEquals(new Path(volumes[1], Constants.EXPORT_FILE), p);

    EasyMock.verify(master, volumeManager);
  }

  @Test
  public void testCreateImportDir() throws Exception {
    Master master = EasyMock.createMock(Master.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager volumeManager = EasyMock.createMock(VolumeManager.class);
    UniqueNameAllocator uniqueNameAllocator = EasyMock.createMock(UniqueNameAllocator.class);

    String[] expDirs = {"hdfs://nn1:8020/import-dir-nn1", "hdfs://nn2:8020/import-dir-nn2",
        "hdfs://nn3:8020/import-dir-nn3"};
    String joinedImpDirs = expDirs[0] + "," + expDirs[1] + "," + expDirs[2];
    String[] tableDirs = {"hdfs://nn1:8020/apps/accumulo1/tables", "hdfs://nn2:8020/applications/accumulo/tables",
        "hdfs://nn3:8020/applications/accumulo"};
    String dirName = "abcd";

    EasyMock.expect(master.getContext()).andReturn(context);
    EasyMock.expect(master.getFileSystem()).andReturn(volumeManager);
    EasyMock.expect(context.getUniqueNameAllocator()).andReturn(uniqueNameAllocator);
    EasyMock.expect(
        volumeManager.matchingFileSystem(EasyMock.eq(new Path(expDirs[0])), EasyMock.eq(tableDirs)))
        .andReturn(new Path(tableDirs[0]));
    EasyMock.expect(
        volumeManager.matchingFileSystem(EasyMock.eq(new Path(expDirs[1])), EasyMock.eq(tableDirs)))
        .andReturn(new Path(tableDirs[1]));
    EasyMock.expect(
        volumeManager.matchingFileSystem(EasyMock.eq(new Path(expDirs[2])), EasyMock.eq(tableDirs)))
        .andReturn(new Path(tableDirs[2]));
    EasyMock.expect(uniqueNameAllocator.getNextName()).andReturn(dirName).times(3);

    ImportedTableInfo ti = new ImportedTableInfo();
    ti.tableId = TableId.of("5b");
    ti.directories = ImportTable.parseExportDir(joinedImpDirs);
    assertEquals(3, ti.directories.size());

    EasyMock.replay(master, context, volumeManager, uniqueNameAllocator);

    CreateImportDir ci = new CreateImportDir(ti);
    ci.create(tableDirs, master);
    assertEquals(3, ti.directories.size());
    for (ImportedTableInfo.DirectoryMapping dm : ti.directories) {
      assertNotNull(dm.exportDir);
      assertNotNull(dm.importDir);
      assertTrue(dm.importDir.contains(Constants.HDFS_TABLES_DIR));
      assertMatchingFilesystem(dm.exportDir, dm.importDir);
      assertTrue(
          dm.importDir.contains(ti.tableId.canonical() + "/" + Constants.BULK_PREFIX + dirName));
    }
    EasyMock.verify(master, context, volumeManager, uniqueNameAllocator);
  }

  private static void assertMatchingFilesystem(String expected, String target) {
    URI uri1 = URI.create(expected);
    URI uri2 = URI.create(target);

    if (uri1.getScheme().equals(uri2.getScheme())) {
      String a1 = uri1.getAuthority();
      String a2 = uri2.getAuthority();
      if ((a1 == null && a2 == null) || (a1 != null && a1.equals(a2))) {
        return;
      }
    }

    fail("Filesystems do not match: " + expected + " vs. " + target);
  }
}
