package smbjoin.beam;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import smbjoin.SerializableSchema;

public class SMBAvroSink extends PTransform<PCollection<SMBFile>, PDone> {

  private ResourceId baseFile;
  private SerializableSchema serializableSchema;

  private SMBAvroSink(ResourceId baseFile, SerializableSchema serializableSchema) {
    this.baseFile = baseFile;
    this.serializableSchema = serializableSchema;
  }

  public static SMBAvroSink create(ResourceId baseFile, SerializableSchema schema) {
    return new SMBAvroSink(baseFile, schema);
  }

  @Override
  public PDone expand(final PCollection<SMBFile> input) {

    input.apply(ParDo.of(new WriteFn(baseFile, serializableSchema)));
    return PDone.in(input.getPipeline());
  }

  private class WriteFn extends DoFn<SMBFile, Void> {
    private final ResourceId baseFile;
    private final SerializableSchema serializableSchema;

    WriteFn(final ResourceId baseFile, final SerializableSchema serializableSchema) {
      this.baseFile = baseFile;
      this.serializableSchema = serializableSchema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {

      int bucketId = c.element().bucketId();
      int shardId = c.element().shardId();

      String shardTemplate = String.format("part-b%05d-s%05d", bucketId, shardId);
      final String suffix = ".avro";

      ResourceId resourceId =
          DefaultFilenamePolicy.fromParams(
                  new DefaultFilenamePolicy.Params()
                      .withBaseFilename(baseFile)
                      .withShardTemplate(shardTemplate)
                      .withSuffix(suffix))
              .unwindowedFilename(0, 1, FileBasedSink.CompressionType.UNCOMPRESSED);


      try (WritableByteChannel channel = FileSystems.create(resourceId, "application/avro");
          DataFileWriter<GenericRecord> dataFileWriter =
          new DataFileWriter<GenericRecord>(new GenericDatumWriter<>())
              .setCodec(CodecFactory.deflateCodec(6))
              .setMeta("smbjoin.bucketId", bucketId)
              .setMeta("smbjoin.shardId", shardId)
              .create(serializableSchema.schema(), Channels.newOutputStream(channel))) {
        for (byte[] t : c.element().values()) {
          dataFileWriter.appendEncoded(ByteBuffer.wrap(t));
        }
      }
    }
  }
}
