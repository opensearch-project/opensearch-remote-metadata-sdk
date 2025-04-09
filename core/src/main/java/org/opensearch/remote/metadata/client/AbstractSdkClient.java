/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.NamedObjectNotFoundException;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.opensearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.ParsedComposite;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilters;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.ParsedGlobal;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.ParsedAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedVariableWidthHistogram;
import org.opensearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.ParsedMissing;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.nested.ParsedNested;
import org.opensearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.opensearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.opensearch.search.aggregations.bucket.range.ParsedDateRange;
import org.opensearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.opensearch.search.aggregations.bucket.range.ParsedRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ParsedAvg;
import org.opensearch.search.aggregations.metrics.ParsedCardinality;
import org.opensearch.search.aggregations.metrics.ParsedExtendedStats;
import org.opensearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.opensearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMedianAbsoluteDeviation;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.opensearch.search.aggregations.metrics.ParsedStats;
import org.opensearch.search.aggregations.metrics.ParsedSum;
import org.opensearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.ParsedTopHits;
import org.opensearch.search.aggregations.metrics.ParsedValueCount;
import org.opensearch.search.aggregations.metrics.ParsedWeightedAvg;
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.opensearch.search.aggregations.pipeline.ParsedDerivative;
import org.opensearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.opensearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.opensearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.opensearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.opensearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;

/**
 * Superclass abstracting privileged action execution
 */
public abstract class AbstractSdkClient implements SdkClientDelegate {

    protected String tenantIdField;

    protected String remoteMetadataType;
    protected String remoteMetadataEndpoint;
    protected String region;
    protected String serviceName;
    protected NamedXContentRegistry defaultXContentRegistry;

    @Override
    public void initialize(Map<String, String> metadataSettings) {
        initialize(null, metadataSettings);
    }

    /**
     * Initialize with an optional xContentRegistry
     * @param xContentRegistry a registry to be used for custom named objects not handled by the default
     * @param metadataSettings the metadata settings
     */
    public void initialize(NamedXContentRegistry xContentRegistry, Map<String, String> metadataSettings) {
        this.tenantIdField = metadataSettings.get(TENANT_ID_FIELD_KEY);

        this.remoteMetadataType = metadataSettings.get(REMOTE_METADATA_TYPE_KEY);
        this.remoteMetadataEndpoint = metadataSettings.get(REMOTE_METADATA_ENDPOINT_KEY);
        this.region = metadataSettings.get(REMOTE_METADATA_REGION_KEY);
        this.serviceName = metadataSettings.get(REMOTE_METADATA_SERVICE_NAME_KEY);

        NamedXContentRegistry defaultRegistry = createXContentRegistry();
        this.defaultXContentRegistry = (xContentRegistry != null)
            ? new CombinedNamedXContentRegistry(defaultRegistry, xContentRegistry)
            : defaultRegistry;
    }

    /**
     * Execute this privileged action asynchronously
     * @param <T> The return type of the completable future to be returned
     * @param action the action to execute
     * @param executor the executor for the action
     * @return A {@link CompletionStage} encapsulating the completable future for the action
     */
    protected <T> CompletionStage<T> executePrivilegedAsync(PrivilegedAction<T> action, Executor executor) {
        return CompletableFuture.supplyAsync(() -> doPrivileged(action), executor);
    }

    private NamedXContentRegistry createXContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(getDefaultNamedXContents());
        return new NamedXContentRegistry(entries);
    }

    private List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = Map.ofEntries(
            Map.entry(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c)),
            Map.entry(WeightedAvgAggregationBuilder.NAME, (p, c) -> ParsedWeightedAvg.fromXContent(p, (String) c)),
            Map.entry(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c)),
            Map.entry(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c)),
            Map.entry(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c)),
            Map.entry(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c)),
            Map.entry(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c)),
            Map.entry(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c)),
            Map.entry(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c)),
            Map.entry(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c)),
            Map.entry(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c)),
            Map.entry(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c)),
            Map.entry(MedianAbsoluteDeviationAggregationBuilder.NAME, (p, c) -> ParsedMedianAbsoluteDeviation.fromXContent(p, (String) c)),
            Map.entry(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c)),
            Map.entry(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c)),
            Map.entry(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c)),
            Map.entry(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c)),
            Map.entry(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c)),
            Map.entry(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c)),
            Map.entry(InternalSampler.NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c)),
            Map.entry(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c)),
            Map.entry(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c)),
            Map.entry(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c)),
            Map.entry(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c)),
            Map.entry(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c)),
            Map.entry(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c)),
            Map.entry(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c)),
            Map.entry(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c)),
            Map.entry(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c)),
            Map.entry(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c)),
            Map.entry(AutoDateHistogramAggregationBuilder.NAME, (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c)),
            Map.entry(VariableWidthHistogramAggregationBuilder.NAME, (p, c) -> ParsedVariableWidthHistogram.fromXContent(p, (String) c)),
            Map.entry(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c)),
            Map.entry(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c)),
            Map.entry(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c)),
            Map.entry(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c)),
            Map.entry(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c)),
            Map.entry(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c)),
            Map.entry(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c)),
            Map.entry(MultiTermsAggregationBuilder.NAME, (p, c) -> ParsedMultiTerms.fromXContent(p, (String) c)),
            Map.entry(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c)),
            Map.entry(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c)),
            Map.entry(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c)),
            Map.entry(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c)),
            Map.entry(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c)),
            Map.entry(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c))
        );

        List<NamedXContentRegistry.Entry> entries = map.entrySet()
            .stream()
            .map((entry) -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField((String) entry.getKey()), entry.getValue()))
            .collect(Collectors.toList());
        return entries;
    }

    private static class CombinedNamedXContentRegistry extends NamedXContentRegistry {
        private final NamedXContentRegistry primaryRegistry;
        private final NamedXContentRegistry backupRegistry;

        /**
         * Create a wrapper NamedXContentRegistry for parsing
         * @param primaryRegistry the registry to attempt to use to parse first
         * @param backupRegistry the backup registry to use if the provided registry doesn't have the named object
         */
        public CombinedNamedXContentRegistry(NamedXContentRegistry primaryRegistry, NamedXContentRegistry backupRegistry) {
            super(Collections.emptyList()); // No-arg constructor not available
            this.primaryRegistry = primaryRegistry;
            this.backupRegistry = backupRegistry;
        }

        @Override
        public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
            try {
                // Attempt to parse first with client provided registry
                return primaryRegistry.parseNamedObject(categoryClass, name, parser, context);
            } catch (NamedObjectNotFoundException e) {
                // If the NamedObject isn't there, try the backup
                return backupRegistry.parseNamedObject(categoryClass, name, parser, context);
            }
        }
    }
}
