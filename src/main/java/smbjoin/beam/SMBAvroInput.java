package smbjoin.beam;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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

  private SMBFileMetadata getBucketingMetadata(
      ResourceId resourceId, SerializableSchema serializableSchema) {
    checkNotNull(resourceId);
    try {
      ReadableByteChannel channel = FileSystems.open(resourceId);
      try (DataFileStream stream =
          new DataFileStream(
              Channels.newInputStream(channel),
              new GenericDatumReader(serializableSchema.schema()))) {
        return SMBFileMetadata.create(
            resourceId,
            stream.getMetaLong("smbjoin.bucketId"),
            stream.getMetaLong("smbjoin.shardId"));
      } finally {
        if (channel.isOpen()) {
          channel.close();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't read file");
    }
  }

  @Override
  public PCollection<KV<K, KV<Iterable<L>, Iterable<R>>>> expand(final PBegin input) {
    return input
        .apply(Create.<Void>of(Collections.singletonList(null)))
        .apply(ParDo.of(new SMBFileIOFn()))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new SortMergeJoinDoFn()))
        .setCoder(
            KvCoder.of(
                leftSMBPartitioning.getJoinKeyCoder(),
                KvCoder.of(
                    IterableCoder.of(leftSMBPartitioning.getRecordCoder()),
                    IterableCoder.of(rightSMBPartitioning.getRecordCoder()))));
  }


  private class SMBFileIOFn extends DoFn<Void, KV<ResourceId, ResourceId>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      final List<SMBFileMetadata> left =
          FileSystems.match(leftSpec).metadata().stream()
              .map(MatchResult.Metadata::resourceId)
              .map(rId -> getBucketingMetadata(rId, leftSchema))
              .collect(Collectors.toList());

      final List<SMBFileMetadata> right =
          FileSystems.match(rightSpec).metadata().stream()
              .map(MatchResult.Metadata::resourceId)
              .map(rId -> getBucketingMetadata(rId, rightSchema))
              .collect(Collectors.toList());

      final long leftBuckets =
          left.stream().mapToLong(SMBFileMetadata::bucketId).max().orElse(0) + 1;
      final long rightBuckets =
          right.stream().mapToLong(SMBFileMetadata::bucketId).max().orElse(0) + 1;

      // Bitwise magic
      if ((leftBuckets & (leftBuckets - 1)) != 0) {
        throw new RuntimeException("Number of buckets on the left should be a power of two.");
      }

      if ((rightBuckets & (rightBuckets - 1)) != 0) {
        throw new RuntimeException("Number of buckets on the right should be a power of two.");
      }

      long numBuckets = Math.min(leftBuckets, rightBuckets);

      System.out.println(
          String.format(
              "Found %d (%d, %d) buckets over (%d, %d) shards",
              numBuckets, leftBuckets, rightBuckets, left.size(), right.size()));
      for (SMBFileMetadata leftIt : left) {
        for (SMBFileMetadata rightIt : right) {
          if (Math.floorMod(leftIt.bucketId(), numBuckets)
              == Math.floorMod(rightIt.bucketId(), numBuckets)) {
            c.output(KV.of(leftIt.resourceId(), rightIt.resourceId()));
          }
        }
      }
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
        int compareResults = comparator.compare(
            encodedGroupKey, partitioning.getEncodedJoinKey(iterator.peek()));
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

    @ProcessElement
    public void processElement(@Element KV<ResourceId, ResourceId> filePair, ProcessContext c)
        throws IOException {
      final ResourceId leftFile = filePair.getKey();
      final ResourceId rightFile = filePair.getValue();

      try (ReadableByteChannel leftChannel = FileSystems.open(leftFile);
          InputStream leftInputStream = Channels.newInputStream(leftChannel);
          DataFileStream<L> leftStream =
              new DataFileStream<>(
                  leftInputStream, new SpecificDatumReader<>(leftSchema.schema()));
          ReadableByteChannel rightChannel = FileSystems.open(rightFile);
          InputStream rightInputStream = Channels.newInputStream(rightChannel);
          DataFileStream<R> rightStream =
              new DataFileStream<>(
                  rightInputStream, new SpecificDatumReader<>(rightSchema.schema()))) {

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
            int compareResults = comparator.compare(
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
