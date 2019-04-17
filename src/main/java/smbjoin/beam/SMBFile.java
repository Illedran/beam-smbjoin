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
public abstract class SMBFile {

  public static Coder<SMBFile> coder() {
    return new AtomicCoder<SMBFile>() {
      @Override
      public void encode(final SMBFile value, final OutputStream outStream) throws IOException {
        VarIntCoder.of().encode(value.bucketId(), outStream);
        VarIntCoder.of().encode(value.shardId(), outStream);
        IterableCoder.of(ByteArrayCoder.of()).encode(value.values(), outStream);
      }

      @Override
      public SMBFile decode(final InputStream inStream) throws IOException {
        int bucketId = VarIntCoder.of().decode(inStream);
        int shardId = VarIntCoder.of().decode(inStream);
        Iterable<byte[]> values = IterableCoder.of(ByteArrayCoder.of()).decode(inStream);

        return SMBFile.create(bucketId, shardId, values);
      }
    };
  }

  public static SMBFile create(int bucketId, int shardId, Iterable<byte[]> values) {
    return new AutoValue_SMBFile(bucketId, shardId, values);
  }

  abstract int bucketId();

  abstract int shardId();

  abstract Iterable<byte[]> values();
}
