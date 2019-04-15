package smbjoin.beam;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class SMBShuffle<JoinKeyT, ValueT>
    extends PTransform<PCollection<ValueT>, PCollection<SMBFileBeam>> {

  private SMBPartitioning<JoinKeyT, ValueT> SMBPartitioning;
  private int numBuckets;

  private SMBShuffle(SMBPartitioning<JoinKeyT, ValueT> SMBPartitioning, int numBuckets) {
    this.SMBPartitioning = SMBPartitioning;
    this.numBuckets = numBuckets;
  }

  public static <JoinKeyT, ValueT> SMBShuffle<JoinKeyT, ValueT> create(
      SMBPartitioning<JoinKeyT, ValueT> SMBPartitioning, int numBuckets) {
    return new SMBShuffle<>(SMBPartitioning, numBuckets);
  }

  @Override
  public PCollection<SMBFileBeam> expand(PCollection<ValueT> input) {
    return input
        .apply(
            "Extract joinKey and serialize",
            MapElements.via(new JoinKeySerializeFn(input.getCoder())))
        .apply(GroupByKey.create())
        .apply(SortValuesBytes.create(BufferedExternalSorter.options().withMemoryMB(1024)))
        .apply("Wrap in SMBFiles", MapElements.via(new WrapSMBFileFn()))
        .setCoder(SMBFileBeam.coder());
  }

  private class JoinKeySerializeFn extends SimpleFunction<ValueT, KV<Integer, KV<byte[], byte[]>>> {
    private Coder<ValueT> inputCoder;

    private JoinKeySerializeFn(Coder<ValueT> inputCoder) {
      this.inputCoder = inputCoder;
    }

    @Override
    public KV<Integer, KV<byte[], byte[]>> apply(final ValueT value) {
      try {
        byte[] encodedJoinKey = SMBPartitioning.getEncodedJoinKey(value);
        int bucketKey = SMBPartitioning.hashEncodedKey(encodedJoinKey);
        byte[] encodedValue = CoderUtils.encodeToByteArray(inputCoder, value);

        return KV.of(Math.floorMod(bucketKey, numBuckets), KV.of(encodedJoinKey, encodedValue));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class WrapSMBFileFn
      extends SimpleFunction<KV<Integer, Iterable<KV<byte[], byte[]>>>, SMBFileBeam> {

    @Override
    public SMBFileBeam apply(final KV<Integer, Iterable<KV<byte[], byte[]>>> input) {
      return SMBFileBeam.create(
          input.getKey(),
          0,
          StreamSupport.stream(input.getValue().spliterator(), false)
              .map(KV::getValue)
              .collect(Collectors.toList()));
    }
  }
}
