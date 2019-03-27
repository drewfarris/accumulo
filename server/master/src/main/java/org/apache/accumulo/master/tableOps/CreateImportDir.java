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

import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CreateImportDir extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(CreateImportDir.class);
  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  CreateImportDir(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {

    String[] tableDirs = ServerConstants.getTablesDirs();

    create(tableDirs, master);

    return new MapImportFileNames(tableInfo);
  }

  void create(String[] tableDirs, Master master) throws IOException {

    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();

    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Path exportDir = new Path(dm.exportDir);

      log.info("Looking for matching filesystem for " + exportDir + " from options "
          + Arrays.toString(tableDirs));
      Path base = master.getFileSystem().matchingFileSystem(exportDir, tableDirs);
      if (base == null) {
        throw new IOException(dm.exportDir + " is not in a volume configured for Accumulo");
      }
      log.info("Chose base table directory of " + base);
      Path directory = new Path(base, tableInfo.tableId);

      Path newBulkDir = new Path(directory, Constants.BULK_PREFIX + namer.getNextName());

      dm.importDir = newBulkDir.toString();

      log.info("Using import dir: " + dm.importDir);
    }
  }
}
