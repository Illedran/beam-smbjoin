package smbjoin.beam;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Given a PCollection<Long> which contains the size of serialized records, returns a
 * PCollection<Integer> containing a single integer for the number of buckets required to fit the
 * data.
 */
@AutoValue
abstract class ComputeNumBuckets extends PTransform<PCollection<Long>, PCollectionView<Integer>> {

  public static ComputeNumBuckets create(int bucketSizeMB) {
    return new AutoValue_ComputeNumBuckets(bucketSizeMB);
  }

  public static ComputeNumBuckets of(int bucketSizeMB) {
    return ComputeNumBuckets.create(bucketSizeMB);
  }

  abstract int bucketSizeMB();

  @Override
  public PCollectionView<Integer> expand(PCollection<Long> input) {
    return input
        .apply(Sum.longsGlobally())
        .apply(
            "Computing number of buckets",
            MapElements.via(
                new SimpleFunction<Long, Integer>() {
                  @Override
                  public Integer apply(Long input) {
                    long approximateBuckets = 1 + input / (bucketSizeMB() * 1024L * 1024L);
                    return 1 << (int) Math.ceil(Math.log(approximateBuckets) / Math.log(2));
                  }
                }))
        .apply(View.asSingleton());
  }
}
