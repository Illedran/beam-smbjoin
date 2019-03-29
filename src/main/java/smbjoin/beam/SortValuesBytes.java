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
package smbjoin.beam;

import java.io.IOException;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class SortValuesBytes<PrimaryKeyT>
    extends PTransform<
        PCollection<KV<PrimaryKeyT, Iterable<KV<byte[], byte[]>>>>,
        PCollection<KV<PrimaryKeyT, Iterable<KV<byte[], byte[]>>>>> {

  private final BufferedExternalSorter.Options sorterOptions;

  private SortValuesBytes(BufferedExternalSorter.Options sorterOptions) {
    this.sorterOptions = sorterOptions;
  }

  public static <PrimaryKeyT> SortValuesBytes<PrimaryKeyT> create(
      BufferedExternalSorter.Options sorterOptions) {
    return new SortValuesBytes<>(sorterOptions);
  }

  @Override
  public PCollection<KV<PrimaryKeyT, Iterable<KV<byte[], byte[]>>>> expand(
      PCollection<KV<PrimaryKeyT, Iterable<KV<byte[], byte[]>>>> input) {
    return input.apply(ParDo.of(new SortValuesDoFn<>(sorterOptions))).setCoder(input.getCoder());
  }

  private static class SortValuesDoFn<PrimaryKeyT>
      extends DoFn<
          KV<PrimaryKeyT, Iterable<KV<byte[], byte[]>>>,
          KV<PrimaryKeyT, Iterable<KV<byte[], byte[]>>>> {
    private final BufferedExternalSorter.Options sorterOptions;

    SortValuesDoFn(BufferedExternalSorter.Options sorterOptions) {
      this.sorterOptions = sorterOptions;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<KV<byte[], byte[]>> records = c.element().getValue();

      try {
        BufferedExternalSorter sorter = BufferedExternalSorter.create(sorterOptions);
        for (KV<byte[], byte[]> record : records) {
          sorter.add(record);
        }
        c.output(KV.of(c.element().getKey(), sorter.sort()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
