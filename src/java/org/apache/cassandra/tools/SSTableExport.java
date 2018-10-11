/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{

    private static final String KEY_OPTION = "k";
    private static final String DEBUG_OUTPUT_OPTION = "d";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String ENUMERATE_KEYS_OPTION = "e";
    private static final String RAW_TIMESTAMPS = "t";
    private static final String MINIMAL_JSON = "mj";
    private static final String START_TOKEN = "st";
    private static final String END_TOKEN = "et";
    private static final String CLUSTERING_START = "cs";
    private static final String CLUSTERING_END = "ce";

    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
        DatabaseDescriptor.toolInitialization();

        Option optKey = new Option(KEY_OPTION, true, "Partition key");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Excluded partition key");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATE_KEYS_OPTION, false, "enumerate partition keys only");
        options.addOption(optEnumerate);

        Option debugOutput = new Option(DEBUG_OUTPUT_OPTION, false, "CQL row per line internal representation");
        options.addOption(debugOutput);

        Option rawTimestamps = new Option(RAW_TIMESTAMPS, false, "Print raw timestamps instead of iso8601 date strings");
        options.addOption(rawTimestamps);

        Option minimalJson = new Option(MINIMAL_JSON, false, "Output one partition per-line as unformatted JSON");
        options.addOption(minimalJson);

        Option startToken = new Option(START_TOKEN, true, "Specify token of the first partition of the dump range");
        options.addOption(startToken);

        Option endToken = new Option(END_TOKEN, true, "Specify token of the partition in which the dump range ends");
        options.addOption(endToken);

        Option clusteringStart = new Option(CLUSTERING_START, true, "Specify clustering key to start dump at in each partition");
        options.addOption(clusteringStart);

        Option clusteringEnd = new Option(CLUSTERING_END, true, "Specify clustering key to end dump at in each partition");
        options.addOption(clusteringEnd);
    }

    /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    public static CFMetaData metadataFromSSTable(Descriptor desc) throws IOException
    {
        if (!desc.version.storeRows())
            throw new IOException("pre-3.0 SSTable is not supported.");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
        builder.addPartitionKey("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        return builder.build();
    }

    private static <T> Stream<T> arrayToStream(T[] items) {
        if (items == null)
            return Stream.empty();

        return Arrays.stream(items);
    }

    private static <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    private static Stream<DecoratedKey> decorateKeys(CFMetaData metadata, Stream<String> keys) {
        return keys
            .map(metadata.getKeyValidator()::fromString)
            .map(metadata.partitioner::decorateKey);
    }

    private static ClusteringBound parseClusteringBound(CFMetaData metadata, String value, boolean isStart,
                                                 boolean isInclusive) {
        CompositeType clusteringKeyType = CompositeType.getInstance(metadata.comparator.subtypes());
        ByteBuffer buf = clusteringKeyType.fromString(value);
        ByteBuffer[] components = clusteringKeyType.split(buf);
        return ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), components);
    }

    private static Stream<AbstractBounds<PartitionPosition>> defaultBounds(CFMetaData metadata) {
        return Stream.of(Range.makeRowRange(metadata.partitioner.getMinimumToken(),
                                            metadata.partitioner.getMaximumToken()));
    }

    private static ClusteringIndexFilter defaultClusteringFilter(CFMetaData metadata) {
        return new ClusteringIndexSliceFilter(Slices.ALL, false);
    }

    private static String prepareToken(String token) {
        if (token.startsWith("\\-"))
            return token.substring(1);
        else
            return token;
    }

    private static Stream<Pair<ISSTableScanner, Stream<UnfilteredRowIterator>>> getFilteredDumpStream(final SSTableReader sstable, CFMetaData metadata) {
        String[] keys = cmd.getOptionValues(KEY_OPTION);
        String[] excludes = cmd.getOptionValues(EXCLUDE_KEY_OPTION);

        String startToken = cmd.getOptionValue(START_TOKEN);
        String endToken = cmd.getOptionValue(END_TOKEN);

        String clusteringStart = cmd.getOptionValue(CLUSTERING_START);
        String clusteringEnd = cmd.getOptionValue(CLUSTERING_END);

        Stream<AbstractBounds<PartitionPosition>> bounds = defaultBounds(metadata);
        ClusteringIndexFilter clusteringFilter = defaultClusteringFilter(metadata);
        Set<DecoratedKey> excludedPartitions = null;

        if (keys != null || excludes != null) {
            if (startToken != null || endToken != null) {
                System.err.println("Key inclusion or exclusion options cannot be combined with a token range");
                return null;
            }

            if (cmd.hasOption(ENUMERATE_KEYS_OPTION)) {
                System.err.println("Enumerate keys option cannot be used with key inclusion or exclusion");
                return null;
            }

            // We only have excludes, so convert the keys early, since we're gonna be using them for
            // frequent comparisons
            if (keys == null) {
                excludedPartitions = decorateKeys(metadata, arrayToStream(excludes))
                    .collect(Collectors.toSet());
            } else {
                Set<String> excludeSet = arrayToStream(excludes).collect(Collectors.toSet());
                Stream<String> filteredkeys = arrayToStream(keys).filter(key -> !excludeSet.contains(key));

                bounds = decorateKeys(metadata, filteredkeys)
                    .sorted()
                    .map(DecoratedKey::getToken)
                    .map(token -> new Bounds<>(token.minKeyBound(), token.maxKeyBound()));

            }
        } else if (startToken != null || endToken != null) {
            Token.TokenFactory tokenFactory = metadata.partitioner.getTokenFactory();
            Token left = startToken == null
                ? metadata.partitioner.getMinimumToken()
                : tokenFactory.fromString(prepareToken(startToken));
            Token right = endToken == null
                ? metadata.partitioner.getMaximumToken()
                : tokenFactory.fromString(prepareToken(endToken));

            System.err.println("Tokens " + left + " " + right);

            bounds = Stream.of(Range.makeRowRange(left, right));
        }

        if (clusteringStart != null || clusteringEnd != null) {
            ClusteringBound clusteringStartBound = clusteringStart == null
                ? ClusteringBound.BOTTOM
                : parseClusteringBound(metadata, clusteringStart, true, true);
            ClusteringBound clusteringEndBound = clusteringEnd == null
                ? ClusteringBound.TOP
                : parseClusteringBound(metadata, clusteringEnd, false, false);

            System.err.println("Clustering Start " + clusteringStartBound);
            System.err.println("Clustering End " + clusteringEndBound);

            Slices slices = Slices.with(metadata.comparator, Slice.make(clusteringStartBound, clusteringEndBound));
            clusteringFilter = new ClusteringIndexSliceFilter(slices, false);
        }

        final Set<DecoratedKey> actualExcludedPartitions = excludedPartitions;
        final ClusteringIndexFilter actualClusteringFilter = clusteringFilter;
        return bounds.map(bound -> {
            ISSTableScanner scanner = sstable.getScanner(
                ColumnFilter.all(metadata), new DataRange(bound, actualClusteringFilter), false,
                SSTableReadsListener.NOOP_LISTENER);

            Stream<UnfilteredRowIterator> rowStream = iterToStream(scanner);
            if (actualExcludedPartitions != null)
                rowStream = rowStream.filter(i -> !actualExcludedPartitions.contains(i.partitionKey()));

            return Pair.create(scanner, rowStream);
        });
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file, export the contents of the SSTable to JSON.
     *
     * @param args
     *            command lines arguments
     * @throws ConfigurationException
     *             on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
    {
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            printUsage();
            System.exit(1);
        }



        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        if (Descriptor.isLegacyFile(new File(ssTableFileName)))
        {
            System.err.println("Unsupported legacy sstable");
            System.exit(1);
        }
        if (!new File(ssTableFileName).exists())
        {
            System.err.println("Cannot find file " + ssTableFileName);
            System.exit(1);
        }

        Descriptor desc = Descriptor.fromFilename(ssTableFileName);
        try
        {
            CFMetaData metadata = metadataFromSSTable(desc);
            if (cmd.hasOption(ENUMERATE_KEYS_OPTION))
            {
                try (KeyIterator iter = new KeyIterator(desc, metadata))
                {
                    JsonTransformer transformer = new JsonTransformer(
                        null, metadata, System.out, cmd.hasOption(RAW_TIMESTAMPS), cmd.hasOption(MINIMAL_JSON));
                    transformer.writeKeys(iterToStream(iter));
                }
            }
            else
            {
                boolean debug = cmd.hasOption(DEBUG_OUTPUT_OPTION);

                SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
                Stream<Pair<ISSTableScanner, Stream<UnfilteredRowIterator>>> scannersPartitions = getFilteredDumpStream(sstable, metadata);
                if (scannersPartitions == null) {
                    printUsage();
                    System.exit(1);
                }

                scannersPartitions.forEach(scannerPartition ->
                {
                    ISSTableScanner scanner = scannerPartition.left;
                    Stream<UnfilteredRowIterator> partitions = scannerPartition.right;

                    try {
                        if (debug)
                        {
                            AtomicLong position = new AtomicLong();
                            partitions.forEach(partition -> {
                                System.err.println("Partition token " + partition.partitionKey().getToken());
                                if (!partition.partitionLevelDeletion().isLive())
                                {
                                    System.out.println("[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "]@" +
                                                       position.get() + " " + partition.partitionLevelDeletion());
                                }
                                if (!partition.staticRow().isEmpty())
                                {
                                    System.out.println("[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "]@" +
                                                       position.get() + " " + partition.staticRow().toString(metadata, true));
                                }
                                partition.forEachRemaining(row ->
                                {
                                    System.out.println(
                                            "[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "]@"
                                                    + position.get() + " " + row.toString(metadata, false, true));
                                });
                            });
                        }
                        else
                        {
                            JsonTransformer transformer = new JsonTransformer(
                                scanner, metadata, System.out, cmd.hasOption(RAW_TIMESTAMPS),
                                cmd.hasOption(MINIMAL_JSON));
                            transformer.writePartitions(partitions);
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                        System.exit(1);
                    }
                });
            }
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
        }

        System.exit(0);
    }

    private static void printUsage()
    {
        String usage = String.format("sstabledump <sstable file path> <options>%n");
        String header = "Dump contents of given SSTable to standard output in JSON format.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
