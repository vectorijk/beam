/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.tez.translation.io;

import java.io.IOException;
import org.apache.beam.runners.tez.translation.TranslatorUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/** {@link TezOutputManager} implementation that properly writes output to {@link MROutput}. */
public class MROutputManager extends TezOutputManager {

  private MROutput output;

  public MROutputManager(LogicalOutput output) {
    super(output);
    if (output.getClass().equals(MROutput.class)) {
      this.output = (MROutput) output;
      try {
        setWriter((KeyValueWriter) output.getWriter());
      } catch (Exception e) {
        throw new RuntimeException("Error when retrieving writer for output" + e.getMessage());
      }
    } else {
      throw new RuntimeException("Incorrect OutputManager for: " + output.getClass());
    }
  }

  @Override
  public void after() {
    try {
      output.flush();
      output.commit();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
    try {
      getWriter().write(null, TranslatorUtil.convertToBytesWritable(output.getValue()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
