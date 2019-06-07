package smbjoin.beam;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jets3t.service.acl.gs.GroupByDomainGrantee;

@AutoValue
public abstract class ResolveSkewnessSize
    extends PTransform<PCollection<KV<Integer, KV<byte[], byte[]>>>, PCollectionView<List<Integer>>> {

  public static ResolveSkewnessSize create(
      PCollectionView<Integer> numBucketsView, int bucketSizeMB, long recordOverhead) {
    return new AutoValue_ResolveSkewnessSize(numBucketsView, bucketSizeMB, recordOverhead);
  }

  abstract PCollectionView<Integer> numBucketsView();
  abstract int bucketSizeMB();
  abstract long recordOverhead();

  @Override
  public PCollectionView<List<Integer>> expand(PCollection<KV<Integer, KV<byte[], byte[]>>> input) {
    return input
        .apply(
            "Partition bucket key",
            ParDo.of(
                new DoFn<KV<Integer, KV<byte[], byte[]>>, KV<Integer, Long>>() {
                  @ProcessElement
                  public void apply(
                      @Element KV<Integer, KV<byte[], byte[]>> value, ProcessContext c) {
                    int bucketId =
                        Math.floorMod(value.getKey(), c.sideInput(numBucketsView()));
                    c.output(
                        KV.of(
                            bucketId,
                            value.getValue().getValue().length + recordOverhead()
                        )
                    );
                  }
                })
                .withSideInputs(numBucketsView())
        )
        .apply("Compute bucket size", Sum.longsPerKey())
        .apply("Compute number of shards",
            MapElements.via(
                new SimpleFunction<KV<Integer, Long>, KV<Void, KV<Integer, Integer>>>() {
                  @Override
                  public KV<Void, KV<Integer, Integer>> apply(final KV<Integer, Long> input) {
                    int approximateShards =
                        Math.toIntExact(1 + input.getValue() / (bucketSizeMB() * 1024L * 1024L));
                    return KV.of(null, KV.of(input.getKey(), approximateShards));
                  }
                }))
        .apply(GroupByKey.create())
        .apply("Flatten to array",
            ParDo.of(
                new DoFn<KV<Void, Iterable<KV<Integer,Integer>>>, List<Integer>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<Void, Iterable<KV<Integer,Integer>>> value, ProcessContext c) {
                    int numBuckets = c.sideInput(numBucketsView());
                    Integer[] shardMap = new Integer[numBuckets];
                    for(KV<Integer, Integer> kvPair: value.getValue()) {
                      shardMap[kvPair.getKey()] = kvPair.getValue();
                    }
                    c.output(ImmutableList.copyOf(shardMap));
                  }
                }).withSideInputs(numBucketsView())
        )
        .apply(View.asSingleton());
  }
}
