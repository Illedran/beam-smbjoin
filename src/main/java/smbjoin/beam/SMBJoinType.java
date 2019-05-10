package smbjoin.beam;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class SMBJoinType {

  private SMBJoinType() {}

  public static <K, L, R> InnerJoin<K, L, R> innerJoin() {
    return InnerJoin.create();
  }

  @AutoValue
  public abstract static class InnerJoin<K, L, R>
      extends PTransform<
          PCollection<KV<K, KV<Iterable<L>, Iterable<R>>>>, PCollection<KV<K, KV<L, R>>>> {

    public static <K, L, R> InnerJoin<K, L, R> create() {
      return new AutoValue_SMBJoinType_InnerJoin<>();
    }

    @Override
    public PCollection<KV<K, KV<L, R>>> expand(
        PCollection<KV<K, KV<Iterable<L>, Iterable<R>>>> input) {
      return input.apply(
          ParDo.of(
              new DoFn<KV<K, KV<Iterable<L>, Iterable<R>>>, KV<K, KV<L, R>>>() {
                @ProcessElement
                public void processElement(
                    @Element KV<K, KV<Iterable<L>, Iterable<R>>> input, ProcessContext c) {
                  K key = input.getKey();
                  Iterable<L> leftGroup = input.getValue().getKey();
                  Iterable<R> rightGroup = input.getValue().getValue();
                  for (L left : leftGroup) {
                    for (R right : rightGroup) {
                      c.output(KV.of(key, KV.of(left, right)));
                    }
                  }
                }
              }));
    }
  }
}
