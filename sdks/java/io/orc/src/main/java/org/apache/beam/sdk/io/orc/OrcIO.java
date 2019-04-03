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
package org.apache.beam.sdk.io.orc;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OrcIO. */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class OrcIO {
  private static final Logger LOG = LoggerFactory.getLogger(OrcIO.class);

  public static Read read(String schema) {
    return new AutoValue_OrcIO_Read.Builder().setSchema(schema).build();
  }

  /** Disallow construction of utility class. */
  private OrcIO() {}

  /** Implementation of. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable
    abstract ValueProvider<String> getFilePattern();

    abstract String getSchema();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilePattern(ValueProvider<String> filepattern);

      abstract Builder setSchema(String schema);

      abstract Read build();
    }

    /** Reads from the given filename or filepattern. */
    public Read from(ValueProvider<String> filepattern) {
      return builder().setFilePattern(filepattern).build();
    }

    /** Like {@link #from(ValueProvider)}. */
    public Read from(String filepattern) {
      return from(ValueProvider.StaticValueProvider.of(filepattern));
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      String pth = "/Users/jkai/projects/string.orc";
      Path p = new Path(pth);
      Configuration c = new Configuration();
      OrcFile.ReaderOptions op = new OrcFile.ReaderOptions(c);
      try {
        Reader reader = OrcFile.createReader(p, op);

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        BytesColumnVector z = (BytesColumnVector) batch.cols[0];

        LOG.info("number of row" + reader.getNumberOfRows());

        while (rows.nextBatch(batch)) {
          for (int r = 0; r < batch.size; ++r) {
            LOG.info("content: " + z.toString(r));
          }
        }
        rows.close();
      } catch (IOException e) {
        LOG.info("error in reader");
      }
      return input.apply(Create.ofProvider(getFilePattern(), StringUtf8Coder.of()));
    }
  }
}
