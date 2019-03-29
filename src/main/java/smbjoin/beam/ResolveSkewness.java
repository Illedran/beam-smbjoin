package smbjoin.beam;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;

public class ResolveSkewness
    extends PTransform<PCollection<Integer>, PCollection<KV<Integer, Integer>>> {

  private PCollectionView<Integer> numBucketsView;
  private double eps;

  private ResolveSkewness(PCollectionView<Integer> numBucketsView, double eps) {
    this.numBucketsView = numBucketsView;
    this.eps = eps;
  }

  public static ResolveSkewness create(PCollectionView<Integer> numBucketsView, double eps) {
    return new ResolveSkewness(numBucketsView, eps);
  }

  @Override
  public PCollection<KV<Integer, Integer>> expand(PCollection<Integer> input) {
    return input
        .apply(
            ParDo.of(
                    new DoFn<Integer, Integer>() {

                      @ProcessElement
                      public void processElement(@Element Integer value, ProcessContext c) {
                        int numBuckets = c.sideInput(numBucketsView);
                        c.output(Math.floorMod(value, numBuckets));
                      }
                    })
                .withSideInputs(numBucketsView))
        .apply(Count.perElement())
        .apply(
            MapElements.via(
                new SimpleFunction<KV<Integer, Long>, KV<Void, KV<Integer, Long>>>() {
                  @Override
                  public KV<Void, KV<Integer, Long>> apply(final KV<Integer, Long> input) {
                    return KV.of(null, input);
                  }
                }))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new ResolveSkewnessFn()).withSideInputs(numBucketsView))
        .apply(Flatten.iterables());
  }

  private class ResolveSkewnessFn
      extends DoFn<KV<Void, Iterable<KV<Integer, Long>>>, Iterable<KV<Integer, Integer>>> {
    @ProcessElement
    public void processElement(
        @Element KV<Void, Iterable<KV<Integer, Long>>> value, ProcessContext c) {
      int numBuckets = c.sideInput(numBucketsView);
      Iterable<KV<Integer, Long>> counts = value.getValue();
      int[] shardsCount = new int[numBuckets];
      long totalRecordCount = 0;
      long totalShards = numBuckets;
      for (int i = 0; i < numBuckets; i++) {
        shardsCount[i] = 1;
      }
      for (KV<Integer, Long> kv : counts) {
        totalRecordCount += kv.getValue();
      }
      boolean done = false;
      while (!done) {
        done = true;
        double thresh = Math.floor((1 + eps) * totalRecordCount / totalShards);
        for (KV<Integer, Long> kv : counts) {
          int bucketId = kv.getKey();
          long bucketCount = kv.getValue();
          if (bucketCount * 1.0 / shardsCount[bucketId] > thresh) {
            done = false;
            shardsCount[bucketId]++;
            totalShards++;
          }
        }
      }
      ArrayList<KV<Integer, Integer>> out = new ArrayList<>(numBuckets);
      for (int i = 0; i < numBuckets; i++) {
        out.add(KV.of(i, shardsCount[i]));
      }
      System.out.println(totalShards);
      c.output(out);
    }
  }
}
