package smbjoin.beam;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
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
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class SMBAutoShuffle<JoinKeyT, ValueT>
    extends PTransform<PCollection<ValueT>, PCollection<SMBFile>> {

  private final int BUCKET_SIZE_MB = 256;
  private SMBPartitioning<JoinKeyT, ValueT> smbPartitioning;
  private TupleTag<KV<Integer, KV<byte[], byte[]>>> withJoinKeyOutput;
  private final TupleTag<Long> recordSizesOutput = new TupleTag<Long>(){};
  private TupleTag<Integer> hashedBucketKeys;
  private PCollectionView<Integer> numBucketsView;
  private PCollectionView<Map<Integer, Integer>> filesPerBucketMapView;
  private double eps;

  private SMBAutoShuffle(SMBPartitioning<JoinKeyT, ValueT> SMBPartitioning, double eps) {
    this.smbPartitioning = SMBPartitioning;
    this.eps = eps;
  }

  public static <JoinKeyT, ValueT> SMBAutoShuffle<JoinKeyT, ValueT> create(
      SMBPartitioning<JoinKeyT, ValueT> SMBPartitioning, double eps) {
    return new SMBAutoShuffle<>(SMBPartitioning, eps);
  }

  @Override
  public PCollection<SMBFile> expand(PCollection<ValueT> input) {
    withJoinKeyOutput = new TupleTag<KV<Integer, KV<byte[], byte[]>>>() {};
    hashedBucketKeys = new TupleTag<Integer>() {};

    PCollectionTuple res1 =
        input.apply(
            "Extract joinKey and serialize",
            ParDo.of(new JoinKeySerializeWithSideOutputFn(input.getCoder()))
                .withOutputTags(
                    withJoinKeyOutput, TupleTagList.of(recordSizesOutput).and(hashedBucketKeys)));

    numBucketsView =
        res1.get(recordSizesOutput)
            .apply(ComputeNumBuckets.of(BUCKET_SIZE_MB));

    filesPerBucketMapView =
        res1.get(hashedBucketKeys)
            .apply(ResolveSkewness.create(numBucketsView, eps))
            .apply(View.asMap());

    return res1.get(withJoinKeyOutput)
        .apply(
            ParDo.of(new RoundRobinShardFn()).withSideInputs(numBucketsView, filesPerBucketMapView))
        .apply(GroupByKey.create())
        .apply(
            SortValuesBytes.create(BufferedExternalSorter.options().withMemoryMB(BUCKET_SIZE_MB)))
        .apply("Wrap in SMBFiles", MapElements.via(new WrapSMBFileFn()))
        .setCoder(SMBFile.coder());
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
        byte[] encodedJoinKey = smbPartitioning.getEncodedJoinKey(value);
        int bucketKey = smbPartitioning.hashEncodedKey(encodedJoinKey);
        byte[] encodedRecord = CoderUtils.encodeToByteArray(inputCoder, value);
        out.get(recordSizesOutput).output(encodedRecord.length + 11 * 8L);
        out.get(hashedBucketKeys).output(bucketKey);
        out.get(withJoinKeyOutput).output(KV.of(bucketKey, KV.of(encodedJoinKey, encodedRecord)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class ComputeNumBucketsFn extends SimpleFunction<Double, Integer> {

    @Override
    public Integer apply(final Double value) {
      return 1
          << Math.round(
              Math.log(Math.ceil(value / (BUCKET_SIZE_MB * 1024L * 1024L))) / Math.log(2));
    }
  }

  private class RoundRobinShardFn
      extends DoFn<KV<Integer, KV<byte[], byte[]>>, KV<KV<Integer, Integer>, KV<byte[], byte[]>>> {

    @ProcessElement
    public void processElement(@Element KV<Integer, KV<byte[], byte[]>> value, ProcessContext c) {
      int numBuckets = c.sideInput(numBucketsView);
      int bucketId = Math.floorMod(value.getKey(), numBuckets);
      int shardId =
          ThreadLocalRandom.current().nextInt(c.sideInput(filesPerBucketMapView).get(bucketId));
      c.output(KV.of(KV.of(bucketId, shardId), value.getValue()));
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
