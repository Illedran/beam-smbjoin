package smbjoin.beam;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.util.VarInt;

@AutoValue
public abstract class SMBFileMetadata {

  public static Coder<SMBFileMetadata> coder() {
    return new AtomicCoder<SMBFileMetadata>() {
      @Override
      public void encode(final SMBFileMetadata value, final OutputStream outStream) throws IOException {
        ResourceIdCoder.of().encode(value.resourceId(), outStream);
        VarIntCoder.of().encode(value.bucketId(), outStream);
        VarIntCoder.of().encode(value.shardId(), outStream);
      }

      @Override
      public SMBFileMetadata decode(final InputStream inStream) throws IOException {
        ResourceId resourceId = ResourceIdCoder.of().decode(inStream);
        int bucketId = VarIntCoder.of().decode(inStream);
        int shardId = VarIntCoder.of().decode(inStream);

        return SMBFileMetadata.create(resourceId, bucketId, shardId);
      }
    };
  }


  abstract ResourceId resourceId();

  abstract int bucketId();

  abstract int shardId();

  public static SMBFileMetadata create(ResourceId resourceId, int bucketId, int shardId) {
    return new AutoValue_SMBFileMetadata(resourceId, bucketId, shardId);
  }


}
