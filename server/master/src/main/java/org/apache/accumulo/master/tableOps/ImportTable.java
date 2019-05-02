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
package org.apache.accumulo.master.tableOps;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportTable extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(ImportTable.class);

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  public ImportTable(String user, String tableName, String exportDir, String namespaceId,
      boolean keepMappings, boolean skipOnline) {
    tableInfo = new ImportedTableInfo();
    tableInfo.tableName = tableName;
    tableInfo.user = user;
    tableInfo.namespaceId = namespaceId;
    tableInfo.keepMappings = keepMappings;
    tableInfo.skipOnline = skipOnline;
    tableInfo.directories = parseExportDir(exportDir);
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    long result = 0;
    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      result += Utils.reserveHdfsDirectory(new Path(dm.exportDir).toString(), tid);
    }
    result += Utils.reserveNamespace(tableInfo.namespaceId, tid, false, true,
        TableOperation.IMPORT);
    return result;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    checkVersions(env);

    // first step is to reserve a table id.. if the machine fails during this step
    // it is ok to retry... the only side effect is that a table id may not be used
    // or skipped

    // assuming only the master process is creating tables

    Utils.idLock.lock();
    try {
      Instance instance = env.getInstance();
      tableInfo.tableId = Utils.getNextTableId(tableInfo.tableName, instance);
      return new ImportSetupPermissions(tableInfo);
    } finally {
      Utils.idLock.unlock();
    }
  }

  public void checkVersions(Master env) throws AcceptableThriftTableOperationException {
    Path exportFilePath = findExportFile(env);

    tableInfo.exportFile = exportFilePath.toString();

    Integer exportVersion = null;
    Integer dataVersion = null;

    try (ZipInputStream zis = new ZipInputStream(env.getFileSystem().open(exportFilePath))) {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_INFO_FILE)) {
          BufferedReader in = new BufferedReader(new InputStreamReader(zis, UTF_8));
          String line = null;
          while ((line = in.readLine()) != null) {
            String sa[] = line.split(":", 2);
            if (sa[0].equals(ExportTable.EXPORT_VERSION_PROP)) {
              exportVersion = Integer.parseInt(sa[1]);
            } else if (sa[0].equals(ExportTable.DATA_VERSION_PROP)) {
              dataVersion = Integer.parseInt(sa[1]);
            }
          }
          break;
        }
      }
    } catch (IOException ioe) {
      log.warn("{}", ioe.getMessage(), ioe);
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Failed to read export metadata " + ioe.getMessage());
    }

    if (exportVersion == null || exportVersion > ExportTable.VERSION)
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Incompatible export version " + exportVersion);

    if (dataVersion == null || dataVersion > ServerConstants.DATA_VERSION)
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Incompatible data version " + dataVersion);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Utils.unreserveHdfsDirectory(new Path(dm.exportDir).toString(), tid);
    }

    Utils.unreserveNamespace(tableInfo.namespaceId, tid, false);
  }

  static List<ImportedTableInfo.DirectoryMapping> parseExportDir(String exportDir) {
    if (exportDir == null || exportDir.isEmpty()) {
      return Collections.emptyList();
    }

    String[] exportDirs = exportDir.split(",");
    List<ImportedTableInfo.DirectoryMapping> dirs = new ArrayList<>(exportDirs.length);
    for (String ed : exportDirs) {
      log.info("Extracted import directory: {}", ed);

      ImportedTableInfo.DirectoryMapping dir = new ImportedTableInfo.DirectoryMapping();
      dir.exportDir = ed;
      dirs.add(dir);
    }
    return dirs;
  }

  Path findExportFile(Master env) throws AcceptableThriftTableOperationException {
    LinkedHashSet<Path> exportFiles = new LinkedHashSet<>();
    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Path exportFilePath = new Path(dm.exportDir, Constants.EXPORT_FILE);
      try {
        if (env.getFileSystem().exists(exportFilePath)) {
          exportFiles.add(exportFilePath);
        }
      } catch (IOException ioe) {
        log.warn("Non-Fatal IOException reading export file: {}", exportFilePath, ioe);
      }
    }

    if (exportFiles.size() > 1) {
      String fileList = Arrays.toString(exportFiles.toArray());
      log.warn("Found multiple export metadata files: " + fileList);
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Found multiple export metadata files: " + fileList);
    } else if (exportFiles.isEmpty()) {
      log.warn("Unable to locate export metadata");
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Unable to locate export metadata");
    }

    return exportFiles.iterator().next();
  }
}
