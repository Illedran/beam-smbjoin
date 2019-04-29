package smbjoin.beam;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

@AutoValue
public abstract class ResolveSkewnessSize
    extends PTransform<PCollection<KV<Integer, Long>>, PCollectionView<Map<Integer, Integer>>> {

  public static ResolveSkewnessSize create(
      PCollectionView<Integer> numBucketsView, int bucketSizeMB) {
    return new AutoValue_ResolveSkewnessSize(numBucketsView, bucketSizeMB);
  }

  abstract PCollectionView<Integer> numBucketsView();

  abstract int bucketSizeMB();

  @Override
  public PCollectionView<Map<Integer, Integer>> expand(PCollection<KV<Integer, Long>> input) {
    return input
        .apply(
            ParDo.of(
                    new DoFn<KV<Integer, Long>, KV<Integer, Long>>() {

                      @ProcessElement
                      public void processElement(
                          @Element KV<Integer, Long> value, ProcessContext c) {
                        int numBuckets = c.sideInput(numBucketsView());
                        c.output(
                            KV.of(Math.floorMod(value.getKey(), numBuckets), value.getValue()));
                      }
                    })
                .withSideInputs(numBucketsView()))
        .apply(Sum.longsPerKey())
        .apply(
            MapElements.via(
                new SimpleFunction<KV<Integer, Long>, KV<Integer, Integer>>() {
                  @Override
                  public KV<Integer, Integer> apply(final KV<Integer, Long> input) {
                    int approximateShards =
                        Math.toIntExact(1 + input.getValue() / (bucketSizeMB() * 1024L * 1024L));
                    return KV.of(input.getKey(), approximateShards);
                  }
                }))
        .apply(View.asMap());
  }
}
