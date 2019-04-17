package smbjoin.beam;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import smbjoin.SerializableSchema;

public class SMBAvroInput<K, L, R>
    extends PTransform<PBegin, PCollection<KV<K, KV<Iterable<L>, Iterable<R>>>>> {

  private final String leftSpec; // gs://.../users/2017-01-01/*.avro
  private final String rightSpec; // gs://.../streams/2017-01-01/*.avro
  private SerializableSchema leftSchema;
  private SerializableSchema rightSchema;
  private SMBPartitioning<K, L> leftSMBPartitioning;
  private SMBPartitioning<K, R> rightSMBPartitioning;

  public SMBAvroInput(
      final String leftSpec,
      final String rightSpec,
      final SerializableSchema leftSchema,
      final SerializableSchema rightSchema,
      final SMBPartitioning<K, L> leftSMBPartitioning,
      final SMBPartitioning<K, R> rightSMBPartitioning) {
    this.leftSpec = leftSpec;
    this.rightSpec = rightSpec;
    this.leftSchema = leftSchema;
    this.rightSchema = rightSchema;
    this.leftSMBPartitioning = leftSMBPartitioning;
    this.rightSMBPartitioning = rightSMBPartitioning;
  }

  public static <K, L, R> SMBAvroInput<K, L, R> create(
      String leftSpec,
      String rightSpec,
      SerializableSchema leftSchema,
      SerializableSchema rightSchema,
      SMBPartitioning<K, L> leftSMBReader,
      SMBPartitioning<K, R> rightSMBReader) {
    return new SMBAvroInput<>(
        leftSpec, rightSpec, leftSchema, rightSchema, leftSMBReader, rightSMBReader);
  }

  @Override
  public PCollection<KV<K, KV<Iterable<L>, Iterable<R>>>> expand(final PBegin input) {
    PCollection<KV<Void, SMBFileMetadata>> left = input.apply("Left: Create match", Create.of(leftSpec))
        .apply("Left: Match files", FileIO.matchAll())
        .apply("Left: Extract metadata", ParDo.of(new ExtractAvroMetadataFn<L>(leftSchema)))
        .setCoder(KvCoder.of(VoidCoder.of(), SMBFileMetadata.coder()));

    PCollection<KV<Void, SMBFileMetadata>> right =
        input
            .apply("Right: Create match", Create.of(rightSpec))
            .apply("Right: Match files", FileIO.matchAll())
        .apply("Right: Extract metadata", ParDo.of(new ExtractAvroMetadataFn<R>(rightSchema)))
        .setCoder(KvCoder.of(VoidCoder.of(), SMBFileMetadata.coder()));

    final TupleTag<SMBFileMetadata> leftTag = new TupleTag<>();
    final TupleTag<SMBFileMetadata> rightTag = new TupleTag<>();

    return KeyedPCollectionTuple
        .of(leftTag, left)
        .and(rightTag, right)
        .apply(CoGroupByKey.create())
        .apply(FlatMapElements.via(new ResolveBucketing(leftTag, rightTag)))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new SortMergeJoinDoFn()))
        .setCoder(
            KvCoder.of(
                leftSMBPartitioning.getJoinKeyCoder(),
                KvCoder.of(
                    IterableCoder.of(leftSMBPartitioning.getRecordCoder()),
                    IterableCoder.of(rightSMBPartitioning.getRecordCoder()))));
  }

  private class ExtractAvroMetadataFn<T> extends DoFn<Metadata, KV<Void, SMBFileMetadata>> {
    private SerializableSchema serializableSchema;
    private DatumReader<T> reader;

    @Setup
    public void setup() {
      this.reader = new SpecificDatumReader<>(serializableSchema.schema());
    }

    ExtractAvroMetadataFn(SerializableSchema serializableSchema) {
      this.serializableSchema = serializableSchema;
    }

    private SMBFileMetadata getBucketingMetadata(
        ResourceId resourceId, DatumReader<T> reader) {
      checkNotNull(resourceId);
      try (ReadableByteChannel channel = FileSystems.open(resourceId);
          InputStream inputStream = Channels.newInputStream(channel);
          DataFileStream<T> stream = new DataFileStream<>(inputStream, reader)) {
        return SMBFileMetadata.create(
            resourceId,
            Integer.parseInt(stream.getMetaString("smbjoin.bucketId")),
            Integer.parseInt(stream.getMetaString("smbjoin.shardId")));
      } catch (IOException e) {
        throw new RuntimeException("Can't read file");
      }
    }

    @ProcessElement
    public void processElement(@Element Metadata input, ProcessContext c) {
      c.output(KV.of(null, getBucketingMetadata(input.resourceId(), reader)));
    }
  }

  private class ResolveBucketing
      extends SimpleFunction<KV<Void, CoGbkResult>, Iterable<KV<ResourceId, ResourceId>>> {

    private TupleTag<SMBFileMetadata> leftTag;
    private TupleTag<SMBFileMetadata> rightTag;

    ResolveBucketing(TupleTag<SMBFileMetadata> leftTag, TupleTag<SMBFileMetadata> rightTag) {
      this.leftTag = leftTag;
      this.rightTag = rightTag;
    }

    @Override
    public Iterable<KV<ResourceId, ResourceId>> apply(final KV<Void, CoGbkResult> input) {
      Iterable<SMBFileMetadata> leftIt = input.getValue().getAll(leftTag);
      Iterable<SMBFileMetadata> rightIt = input.getValue().getAll(rightTag);

      ArrayList<KV<ResourceId, ResourceId>> result = new ArrayList<>();

      long leftBuckets = 0;
      long rightBuckets = 0;
      long leftShards = 0;
      long rightShards = 0;
      for (SMBFileMetadata left : leftIt) {
        leftBuckets = Math.max(leftBuckets, left.bucketId() + 1);
        leftShards++;
      }
      for (SMBFileMetadata right : rightIt) {
        rightBuckets = Math.max(rightBuckets, right.bucketId() + 1);
        rightShards++;
      }

      long numBuckets = Math.min(leftBuckets, rightBuckets);

      System.out.println(
          String.format(
              "Found %d (%d, %d) buckets over (%d, %d) shards",
              numBuckets, leftBuckets, rightBuckets, leftShards, rightShards));

      // Bitwise magic
      if ((leftBuckets & (leftBuckets - 1)) != 0) {
        throw new RuntimeException("Number of buckets on the left should be a power of two.");
      }

      if ((rightBuckets & (rightBuckets - 1)) != 0) {
        throw new RuntimeException("Number of buckets on the right should be a power of two.");
      }

      for (SMBFileMetadata left : leftIt) {
        for (SMBFileMetadata right : rightIt) {
          if (Math.floorMod(left.bucketId(), numBuckets)
              == Math.floorMod(right.bucketId(), numBuckets)) {
            result.add(KV.of(left.resourceId(), right.resourceId()));
          }
        }
      }

      return result;
    }
  }

  private class SortMergeJoinDoFn
      extends DoFn<KV<ResourceId, ResourceId>, KV<K, KV<Iterable<L>, Iterable<R>>>> {

    private Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

    private <T> K consumeIterator(
        PeekingIterator<T> iterator, ArrayList<T> buffer, SMBPartitioning<K, T> partitioning) {
      K groupKey = partitioning.getJoinKey(iterator.peek());
      byte[] encodedGroupKey = partitioning.encodeJoinKey(groupKey);
      buffer.add(iterator.next());

      boolean done = false;
      while (iterator.hasNext() && !done) {
        int compareResults =
            comparator.compare(encodedGroupKey, partitioning.getEncodedJoinKey(iterator.peek()));
        if (compareResults < 0) {
          done = true;
        } else if (compareResults == 0) {
          buffer.add(iterator.next());
        } else {
          throw new RuntimeException("Data is not sorted");
        }
      }
      return groupKey;
    }

    private SpecificDatumReader<L> leftReader;
    private SpecificDatumReader<R> rightReader;

    @Setup
    public void setup() {
      this.leftReader = new SpecificDatumReader<>(leftSchema.schema());
      this.rightReader = new SpecificDatumReader<>(rightSchema.schema());
    }

    @ProcessElement
    public void processElement(@Element KV<ResourceId, ResourceId> filePair, ProcessContext c)
        throws IOException {
      final ResourceId leftFile = filePair.getKey();
      final ResourceId rightFile = filePair.getValue();

      try (ReadableByteChannel leftChannel = FileSystems.open(leftFile);
          InputStream leftInputStream = Channels.newInputStream(leftChannel);
          DataFileStream<L> leftStream = new DataFileStream<>(leftInputStream, leftReader);
          ReadableByteChannel rightChannel = FileSystems.open(rightFile);
          InputStream rightInputStream = Channels.newInputStream(rightChannel);
          DataFileStream<R> rightStream = new DataFileStream<>(rightInputStream, rightReader)) {

        ArrayList<L> leftBuffer = new ArrayList<>();
        ArrayList<R> rightBuffer = new ArrayList<>();
        PeekingIterator<L> leftIt = Iterators.peekingIterator(leftStream);
        PeekingIterator<R> rightIt = Iterators.peekingIterator(rightStream);

        K groupKey;

        while (leftIt.hasNext() || rightIt.hasNext()) {
          leftBuffer.clear();
          rightBuffer.clear();

          if (leftIt.hasNext() && !rightIt.hasNext()) { // Right is empty, left outer join
            groupKey = consumeIterator(leftIt, leftBuffer, leftSMBPartitioning);
          } else if (!leftIt.hasNext() && rightIt.hasNext()) { // Left is empty, right outer join
            groupKey = consumeIterator(rightIt, rightBuffer, rightSMBPartitioning);
          } else {
            int compareResults =
                comparator.compare(
                    leftSMBPartitioning.getEncodedJoinKey(leftIt.peek()),
                    rightSMBPartitioning.getEncodedJoinKey(rightIt.peek()));
            if (compareResults < 0) {
              groupKey = consumeIterator(leftIt, leftBuffer, leftSMBPartitioning);
            } else if (compareResults == 0) {
              groupKey = consumeIterator(leftIt, leftBuffer, leftSMBPartitioning);
              consumeIterator(rightIt, rightBuffer, rightSMBPartitioning);
            } else {
              groupKey = consumeIterator(rightIt, rightBuffer, rightSMBPartitioning);
            }
          }
          c.output(
              KV.of(groupKey, KV.of(new ArrayList<>(leftBuffer), new ArrayList<>(rightBuffer))));
        }
      }
    }
  }
}
