package smbjoin.beam;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;

@AutoValue
public abstract class SMBBucketShardKey {

  public static Coder<SMBBucketShardKey> coder() {
    return new AtomicCoder<SMBBucketShardKey>() {
      @Override
      public void encode(final SMBBucketShardKey value, final OutputStream outStream) throws IOException {
        VarIntCoder.of().encode(value.bucketId(), outStream);
        VarIntCoder.of().encode(value.shardId(), outStream);
      }

      @Override
      public SMBBucketShardKey decode(final InputStream inStream) throws IOException {
        int bucketId = VarIntCoder.of().decode(inStream);
        int shardId = VarIntCoder.of().decode(inStream);

        return SMBBucketShardKey.create(resourceId, bucketId, shardId);
      }
    };
  }



  abstract int bucketId();

  abstract int shardId();

  public static SMBBucketShardKey create(int bucketId, int shardId) {
    return new AutoValue_SMBBucketShardKey(bucketId, shardId);
  }


}
