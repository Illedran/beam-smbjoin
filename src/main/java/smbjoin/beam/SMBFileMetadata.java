package smbjoin.beam;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.fs.ResourceId;

@AutoValue
public abstract class SMBFileMetadata {

  abstract ResourceId resourceId();

  abstract long bucketId();

  abstract long shardId();

  public static SMBFileMetadata create(ResourceId resourceId, long bucketId, long shardId) {
    return new AutoValue_SMBFileMetadata(resourceId, bucketId, shardId);
  }

}
