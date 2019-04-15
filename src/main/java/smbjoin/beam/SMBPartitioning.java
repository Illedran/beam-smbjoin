package smbjoin.beam;

import com.google.common.hash.Hashing;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;

public interface SMBPartitioning<JoinKeyT, ValueT> extends Serializable {
  Coder<JoinKeyT> getJoinKeyCoder();

  default byte[] getEncodedJoinKey(ValueT value) {
    try {

      return CoderUtils.encodeToByteArray(getJoinKeyCoder(), getJoinKey(value));
    } catch (Exception e) {
      throw new RuntimeException("bucketHash failed", e);
    }
  }

  default int hashEncodedKey(byte[] encodedJoinKey) {
    return Hashing.murmur3_32().hashBytes(encodedJoinKey).asInt();
  }

  JoinKeyT getJoinKey(ValueT value);
}
