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

@AutoValue
public abstract class SMBFileBeam {

  public static Coder<SMBFileBeam> coder() {
    return new AtomicCoder<SMBFileBeam>() {
      @Override
      public void encode(final SMBFileBeam value, final OutputStream outStream) throws IOException {
        VarIntCoder.of().encode(value.bucketId(), outStream);
        VarIntCoder.of().encode(value.shardId(), outStream);
        IterableCoder.of(ByteArrayCoder.of()).encode(value.values(), outStream);
      }

      @Override
      public SMBFileBeam decode(final InputStream inStream) throws IOException {
        int bucketId = VarIntCoder.of().decode(inStream);
        int shardId = VarIntCoder.of().decode(inStream);
        Iterable<byte[]> values = IterableCoder.of(ByteArrayCoder.of()).decode(inStream);

        return SMBFileBeam.create(bucketId, shardId, values);
      }
    };
  }

  public static SMBFileBeam create(int bucketId, int shardId, Iterable<byte[]> values) {
    return new AutoValue_SMBFileBeam(bucketId, shardId, values);
  }

  abstract int bucketId();

  abstract int shardId();

  abstract Iterable<byte[]> values();
}
