package smbjoin.beam;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.fs.ResourceId;

@AutoValue
public abstract class SMBFileMetadata {

  public static SMBFileMetadata create(ResourceId resourceId, long bucketId, long shardId) {
    return new AutoValue_SMBFileMetadata(resourceId, bucketId, shardId);
  }

  abstract ResourceId resourceId();

  abstract long bucketId();

  abstract long shardId();
}
