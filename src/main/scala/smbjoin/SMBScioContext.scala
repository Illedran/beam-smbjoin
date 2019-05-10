package smbjoin

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import smbjoin.beam.{SMBAvroInput, SMBJoinType}

object SMBScioContext {

  import scala.language.implicitConversions

  implicit def toSMBScioContext(c: ScioContext): SMBScioContext =
    new SMBScioContext(c)

}

class SMBScioContext(@transient val self: ScioContext) {

  def avroSmbFileInnerJoin[K: Coder, L: Coder, R: Coder](
    leftSpec: String,
    rightSpec: String,
    leftSchema: Schema,
    rightSchema: Schema,
    leftKeyFn: L => K,
    rightKeyFn: R => K
  ): SCollection[(K, (L, R))] = {

    self
      .customInput(
        "SMBRead",
        SMBAvroInput.create(
          leftSpec,
          rightSpec,
          leftSchema,
          rightSchema,
          AvroSMBUtils.getAvroSMBPartitioning(leftSchema, leftKeyFn),
          AvroSMBUtils.getAvroSMBPartitioning(rightSchema, rightKeyFn)
        )
      )
      .applyTransform(SMBJoinType.innerJoin[K, L, R])
      .map { kv =>
        val joined = kv.getValue
        (kv.getKey, (joined.getKey, joined.getValue))
      }
  }
}
