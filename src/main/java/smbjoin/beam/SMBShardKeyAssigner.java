package smbjoin.beam;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class SMBShardKeyAssigner {

  private SMBShardKeyAssigner() {}

  public static Random random(
      PCollectionView<List<Integer>> filesPerBucketMapView) {
    return Random.create(filesPerBucketMapView);
  }

//  public static RoundRobin roundRobin(
//      PCollectionView<List<Integer>> filesPerBucketMapView) {
//    return RoundRobin.create(filesPerBucketMapView);
//  }
//
//  @AutoValue
//  public abstract static class RoundRobin
//      extends PTransform<
//          PCollection<KV<Integer, KV<byte[], byte[]>>>,
//          PCollection<KV<KV<Integer, Integer>, KV<byte[], byte[]>>>> {
//
//    abstract PCollectionView<List<Integer>> filesPerBucketMapView();
//
//    public static RoundRobin create(PCollectionView<List<Integer>> filesPerBucketMapView) {
//      return new AutoValue_SMBShardKeyAssigner_RoundRobin(filesPerBucketMapView);
//    }
//
//
//    @Override
//    public PCollection<KV<KV<Integer, Integer>, KV<byte[], byte[]>>> expand(
//        PCollection<KV<Integer, KV<byte[], byte[]>>> input) {
//      return input.apply("RoundRobin shard assignment",
//          ParDo.of(
//                  new DoFn<
//                      KV<Integer, KV<byte[], byte[]>>,
//                      KV<KV<Integer, Integer>, KV<byte[], byte[]>>>() {
//                    private transient Map<Integer, Integer> shardIdMap;
//
//                    @Setup
//                    public void setup() {
//                      shardIdMap = new HashMap<>();
//                    }
//
//                    @ProcessElement
//                    public void processElement(
//                        @Element KV<Integer, KV<byte[], byte[]>> value, ProcessContext c) {
//                      Map<Integer, Integer> shardsPerBucket = c.sideInput(filesPerBucketMapView());
//                      int numBuckets = shardsPerBucket.size();
//                      int bucketId = Math.floorMod(value.getKey(), numBuckets);
//
//                      int shardId = shardIdMap.getOrDefault(bucketId, 0);
//                      shardIdMap.put(
//                          bucketId,
//                          Math.floorMod(
//                              shardId + 1,
//                             shardsPerBucket.get(bucketId)));
//                      c.output(KV.of(KV.of(bucketId, shardId), value.getValue()));
//                    }
//                  })
//              .withSideInputs(filesPerBucketMapView()));
//    }
//  }

  @AutoValue
  public abstract static class Random
      extends PTransform<
          PCollection<KV<Integer, KV<byte[], byte[]>>>,
          PCollection<KV<KV<Integer, Integer>, KV<byte[], byte[]>>>> {


    abstract PCollectionView<List<Integer>> filesPerBucketMapView();

    public static Random create(PCollectionView<List<Integer>> filesPerBucketMapView) {
      return new AutoValue_SMBShardKeyAssigner_Random(filesPerBucketMapView);
    }


    @Override
    public PCollection<KV<KV<Integer, Integer>, KV<byte[], byte[]>>> expand(
        PCollection<KV<Integer, KV<byte[], byte[]>>> input) {
      return input.apply("Random shard assignment",
          ParDo.of(
                  new DoFn<
                      KV<Integer, KV<byte[], byte[]>>,
                      KV<KV<Integer, Integer>, KV<byte[], byte[]>>>() {

                    @ProcessElement
                    public void processElement(
                        @Element KV<Integer, KV<byte[], byte[]>> value, ProcessContext c) {
                      List<Integer> shardsPerBucket = c.sideInput(filesPerBucketMapView());
                      int numBuckets = shardsPerBucket.size();
                      int bucketId = Math.floorMod(value.getKey(), numBuckets);
                      int shardId = Math.floorMod(ThreadLocalRandom.current().nextInt(), shardsPerBucket.get(bucketId));
                      c.output(KV.of(KV.of(bucketId, shardId), value.getValue()));
                    }
                  })
              .withSideInputs(filesPerBucketMapView()));
    }
  }
}
