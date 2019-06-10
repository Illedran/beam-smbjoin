package smbjoin.beam;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
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
import org.apache.beam.sdk.values.PCollectionView;

@AutoValue
public abstract class SMBSizeShuffle<JoinKeyT, ValueT>
    extends PTransform<PCollection<ValueT>, PCollection<SMBFile>> {

  /** Copied from InMemorySorter: 13 words of 8 bytes each. */
  public static final long DEFAULT_BYTE_OVERHEAD_PER_RECORD = 13 * 8;

  public static <JoinKeyT, ValueT> SMBSizeShuffle<JoinKeyT, ValueT> create(
      int bucketSizeMB, SMBPartitioning<JoinKeyT, ValueT> smbPartitioning, int recordOverhead) {
    return SMBSizeShuffle.<JoinKeyT, ValueT>builder()
        .bucketSizeMB(bucketSizeMB)
        .smbPartitioning(smbPartitioning)
        .recordOverhead(recordOverhead)
        .build();
  }

  public static <JoinKeyT, ValueT> Builder<JoinKeyT, ValueT> builder() {
    return new AutoValue_SMBSizeShuffle.Builder<JoinKeyT, ValueT>()
        .recordOverhead(DEFAULT_BYTE_OVERHEAD_PER_RECORD);
  }

  abstract int bucketSizeMB();

  abstract SMBPartitioning<JoinKeyT, ValueT> smbPartitioning();

  abstract long recordOverhead();

  @Override
  public PCollection<SMBFile> expand(PCollection<ValueT> input) {
    Coder<ValueT> inputCoder = input.getCoder();

    PCollection<KV<Integer, KV<byte[], byte[]>>> encoded =
        input.apply(
            "Extract joinKey and serialize",
            MapElements.via(
                new SimpleFunction<ValueT, KV<Integer, KV<byte[], byte[]>>>() {
                  @Override
                  public KV<Integer, KV<byte[], byte[]>> apply(ValueT input) {
                    try {
                      byte[] encodedJoinKey = smbPartitioning().getEncodedJoinKey(input);
                      int bucketKey = smbPartitioning().hashEncodedKey(encodedJoinKey);
                      byte[] encodedRecord = CoderUtils.encodeToByteArray(inputCoder, input);

                      return KV.of(bucketKey, KV.of(encodedJoinKey, encodedRecord));

                    } catch (CoderException e) {
                      throw new RuntimeException(e);
                    }
                  }
                }));

    // Compute number of buckets
    PCollectionView<Integer> numBucketsView =
        encoded
            .apply(
                "Estimate record size",
                MapElements.via(
                    new SimpleFunction<KV<Integer, KV<byte[], byte[]>>, Long>() {
                      @Override
                      public Long apply(KV<Integer, KV<byte[], byte[]>> input) {
                        return input.getValue().getValue().length + recordOverhead();
                      }
                    }))
            .apply("Compute number of buckets", ComputeNumBuckets.of(bucketSizeMB()));

    // Compute shards per bucket
    final PCollectionView<List<Integer>> filesPerBucketMapView =
        encoded
            .apply("Compute shards per bucket", ResolveSkewnessSize.create(numBucketsView, bucketSizeMB(), recordOverhead()));

    return encoded
        .apply(SMBShardKeyAssigner.random(filesPerBucketMapView))
        .apply(GroupByKey.create())
        .apply(SortValuesBytes.create(BufferedExternalSorter.options().withMemoryMB(1024)))
        .apply("Wrap in SMBFiles", MapElements.via(new WrapSMBFileFn()))
        .setCoder(SMBFile.coder());
  }

  @AutoValue.Builder
  public abstract static class Builder<JoinKeyT, ValueT> {

    public abstract Builder<JoinKeyT, ValueT> bucketSizeMB(int bucketSizeMB);

    public abstract Builder<JoinKeyT, ValueT> smbPartitioning(
        SMBPartitioning<JoinKeyT, ValueT> smbPartitioning);

    public abstract Builder<JoinKeyT, ValueT> recordOverhead(long recordOverhead);

    public abstract SMBSizeShuffle<JoinKeyT, ValueT> build();
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
