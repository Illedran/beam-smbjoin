package smbjoin

import java.io.{
  IOException,
  ObjectInputStream,
  ObjectOutputStream,
  Serializable
}

import org.apache.avro.Schema

object SerializableSchema {

  import scala.language.implicitConversions

  implicit def toSerializableSchema(schema: Schema): SerializableSchema =
    new SerializableSchema(schema)
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
@SerialVersionUID(1L)
class SerializableSchema(@transient var schema: Schema) extends Serializable {

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(schema.toString)
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream): Unit = {
    val strSchema = in.readObject.asInstanceOf[String]
    this.schema = new Schema.Parser().parse(strSchema)
  }
}
