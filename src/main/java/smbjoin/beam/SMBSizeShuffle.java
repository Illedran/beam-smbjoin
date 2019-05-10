package smbjoin.beam;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

@AutoValue
public abstract class SMBSizeShuffle<JoinKeyT, ValueT>
    extends PTransform<PCollection<ValueT>, PCollection<SMBFile>> {

  /** Copied from InMemorySorter: 13 words of 8 bytes each. */
  public final static int DEFAULT_BYTE_OVERHEAD_PER_RECORD = 13 * 8;

  private final TupleTag<KV<Integer, KV<byte[], byte[]>>> withJoinKeyOutput =
      new TupleTag<KV<Integer, KV<byte[], byte[]>>>(){};
  private final TupleTag<Long> recordSizeSideOutput
      = new TupleTag<Long>(){};
  private final TupleTag<KV<Integer, Long>> hashedBucketKeysWithSizeSideOutput
      = new TupleTag<KV<Integer, Long>>(){};


  abstract int bucketSizeMB();
  abstract SMBPartitioning<JoinKeyT, ValueT> smbPartitioning();
  abstract int recordOverhead();

  public static <JoinKeyT, ValueT> SMBSizeShuffle<JoinKeyT, ValueT> create(int bucketSizeMB,
      SMBPartitioning<JoinKeyT, ValueT> smbPartitioning, int recordOverhead) {
    return SMBSizeShuffle.<JoinKeyT, ValueT>builder()
        .bucketSizeMB(bucketSizeMB)
        .smbPartitioning(smbPartitioning)
        .recordOverhead(recordOverhead)
        .build();
  }

  @Override
  public PCollection<SMBFile> expand(PCollection<ValueT> input) {
    PCollectionTuple res1 =
        input.apply(
            "Extract joinKey and serialize",
            ParDo.of(new JoinKeySerializeWithSideOutputFn(input.getCoder()))
                .withOutputTags(
                    withJoinKeyOutput,
                    TupleTagList.of(recordSizeSideOutput).and(hashedBucketKeysWithSizeSideOutput)));

    // Compute number of buckets
    PCollectionView<Integer> numBucketsView =
        res1.get(recordSizeSideOutput)
            .apply(ComputeNumBuckets.of(bucketSizeMB()));

    // Compute shards per bucket
    PCollectionView<Map<Integer, Integer>> filesPerBucketMapView =
        res1.get(hashedBucketKeysWithSizeSideOutput)
            .apply(ResolveSkewnessSize.create(numBucketsView, bucketSizeMB()));

    return res1.get(withJoinKeyOutput)
        .apply(SMBShardKeyAssigner.roundRobin(numBucketsView, filesPerBucketMapView))
        .apply(GroupByKey.create())
        .apply(
            SortValuesBytes.create(BufferedExternalSorter.options().withMemoryMB(bucketSizeMB())))
        .apply("Wrap in SMBFiles", MapElements.via(new WrapSMBFileFn()))
        .setCoder(SMBFile.coder());
  }

  public static <JoinKeyT, ValueT> Builder<JoinKeyT, ValueT> builder() {
    return new AutoValue_SMBSizeShuffle.Builder<JoinKeyT, ValueT>()
        .recordOverhead(DEFAULT_BYTE_OVERHEAD_PER_RECORD);
  }

  @AutoValue.Builder
  public abstract static class Builder<JoinKeyT, ValueT> {

    public abstract Builder<JoinKeyT, ValueT> bucketSizeMB(int bucketSizeMB);

    public abstract Builder<JoinKeyT, ValueT> smbPartitioning(
        SMBPartitioning<JoinKeyT, ValueT> smbPartitioning);

    public abstract Builder<JoinKeyT, ValueT> recordOverhead(int recordOverhead);

    public abstract SMBSizeShuffle<JoinKeyT, ValueT> build();
  }


  private class JoinKeySerializeWithSideOutputFn
      extends DoFn<ValueT, KV<Integer, KV<byte[], byte[]>>> {
    private Coder<ValueT> inputCoder;

    private JoinKeySerializeWithSideOutputFn(Coder<ValueT> inputCoder) {
      this.inputCoder = inputCoder;
    }

    @ProcessElement
    public void processElement(@Element ValueT value, MultiOutputReceiver out) {
      try {
        byte[] encodedJoinKey = smbPartitioning().getEncodedJoinKey(value);
        int bucketKey = smbPartitioning().hashEncodedKey(encodedJoinKey);
        byte[] encodedRecord = CoderUtils.encodeToByteArray(inputCoder, value);
        long encodedSizeWithOverhead = encodedRecord.length + recordOverhead();

        out.get(recordSizeSideOutput).output(encodedSizeWithOverhead);
        out.get(hashedBucketKeysWithSizeSideOutput).output(KV.of(bucketKey, encodedSizeWithOverhead));
        out.get(withJoinKeyOutput).output(KV.of(bucketKey, KV.of(encodedJoinKey, encodedRecord)));

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class WrapSMBFileFn
      extends SimpleFunction<KV<KV<Integer, Integer>, Iterable<KV<byte[], byte[]>>>, SMBFile> {

    @Override
    public SMBFile apply(final KV<KV<Integer, Integer>, Iterable<KV<byte[], byte[]>>> input) {
      return SMBFile.create(
          input.getKey().getKey(),
          input.getKey().getValue(),
          StreamSupport.stream(input.getValue().spliterator(), false)
              .map(KV::getValue)
              .collect(Collectors.toList()));
    }
  }
}
