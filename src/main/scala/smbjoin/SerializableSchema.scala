package smbjoin

import java.io.Serializable

import com.google.common.base.{Function, Supplier, Suppliers}
import org.apache.avro.Schema

object SerializableSchema {
  def of(schema: Schema): Supplier[Schema] = {
    val json = schema.toString
    Suppliers.memoize(
      Suppliers.compose(new StringToSchema, Suppliers.ofInstance(json))
    )
  }
}

private class StringToSchema
  extends Function[String, Schema]
    with Serializable {
  override def apply(input: String): Schema = new Schema.Parser().parse(input)
}
