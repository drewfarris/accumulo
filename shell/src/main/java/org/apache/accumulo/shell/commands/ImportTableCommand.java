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
package org.apache.accumulo.shell.commands;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ImportTableCommand extends Command {

  private Option keepMappingsOption;
  private Option skipOnlineOption;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException {

    boolean keepMappings = false;
    boolean skipOnline = false;

    if (cl.hasOption(keepMappingsOption.getOpt())) {
      keepMappings = true;
    }

    if (cl.hasOption(skipOnlineOption.getOpt())) {
      skipOnline = true;
    }

    shellState.getAccumuloClient().tableOperations().importTable(cl.getArgs()[0], cl.getArgs()[1],
        keepMappings, skipOnline);
    return 0;
  }

  @Override
  public String usage() {
    return getName() + " <table name> <import dir>";
  }

  @Override
  public String description() {
    return "imports a table";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    keepMappingsOption = new Option("k", "keepMappings", false,
        "keep the mappings files generated in the import process");
    o.addOption(keepMappingsOption);
    skipOnlineOption = new Option("s", "skipOnline", false,
        "do not put the table online after the import is complete");
    o.addOption(skipOnlineOption);
    return o;
  }

  @Override
  public int numArgs() {
    return 2;
  }
}
