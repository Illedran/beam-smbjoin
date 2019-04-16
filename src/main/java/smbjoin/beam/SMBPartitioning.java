package smbjoin.beam;

import com.google.common.hash.Hashing;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;

/**
 * @param <JoinKeyT> Type of the join key. Its byte representation is hashed to produce the bucket
 *     id.
 * @param <ValueT> Type of the records.
 */
public interface SMBPartitioning<JoinKeyT, ValueT> extends Serializable {
  Coder<JoinKeyT> getJoinKeyCoder();

  JoinKeyT getJoinKey(ValueT value);

  default byte[] getEncodedJoinKey(ValueT value) {
    try {
      // Encode the key to use .hashBytes later
      return CoderUtils.encodeToByteArray(getJoinKeyCoder(), getJoinKey(value));
    } catch (Exception e) {
      throw new RuntimeException("Hashing failed", e);
    }
  }

  default int hashEncodedKey(byte[] encodedJoinKey) {
    return Hashing.murmur3_32().hashBytes(encodedJoinKey).asInt();
  }
}
