/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.UncategorizedExecutionException;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.DistanceFeatureQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.FieldMaskingSpanQueryBuilder;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.index.query.GeoDistanceQueryBuilder;
import org.opensearch.index.query.GeoPolygonQueryBuilder;
import org.opensearch.index.query.GeoShapeQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.IntervalQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MoreLikeThisQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SpanContainingQueryBuilder;
import org.opensearch.index.query.SpanFirstQueryBuilder;
import org.opensearch.index.query.SpanNearQueryBuilder;
import org.opensearch.index.query.SpanNotQueryBuilder;
import org.opensearch.index.query.SpanOrQueryBuilder;
import org.opensearch.index.query.SpanTermQueryBuilder;
import org.opensearch.index.query.SpanWithinQueryBuilder;
import org.opensearch.index.query.TemplateQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.TermsSetQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.remote.metadata.client.BulkDataObjectResponse;
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.PutDataObjectResponse;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectResponse;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;

/**
 * Utility methods for client implementations
 */
public class SdkClientUtils {
    private static final Logger log = LogManager.getLogger(SdkClientUtils.class);

    private static final NamedXContentRegistry DEFAULT_XCONTENT_REGISTRY = createDefaultXContentRegistry();

    private SdkClientUtils() {}

    /**
     * Wraps the completion of a PUT operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed IndexResponse or any errors
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link OpenSearchStatusException} and {@link CompletionException}.
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    @SafeVarargs
    public static BiConsumer<PutDataObjectResponse, Throwable> wrapPutCompletion(
        ActionListener<IndexResponse> listener,
        Class<? extends Throwable>... exceptionTypesToUnwrap
    ) {
        return (r, throwable) -> {
            if (throwable != null) {
                handleThrowable(listener, throwable, exceptionTypesToUnwrap);
                return;
            }
            IndexResponse indexResponse = r.indexResponse();
            if (indexResponse == null) {
                handleParseFailure(listener, "index");
                return;
            }
            log.info("Put success for {} in {}", indexResponse.getId(), indexResponse.getIndex());
            listener.onResponse(indexResponse);
        };
    }

    /**
     * Wraps the completion of a GET operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed GetResponse or any errors
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link OpenSearchStatusException} and {@link CompletionException}.
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    @SafeVarargs
    public static BiConsumer<GetDataObjectResponse, Throwable> wrapGetCompletion(
        ActionListener<GetResponse> listener,
        Class<? extends Throwable>... exceptionTypesToUnwrap
    ) {
        return (r, throwable) -> {
            if (throwable != null) {
                handleThrowable(listener, throwable, exceptionTypesToUnwrap);
                return;
            }
            GetResponse getResponse = r.getResponse();
            if (getResponse == null) {
                handleParseFailure(listener, "get");
                return;
            }
            log.info("Get success for {} in {}", getResponse.getId(), getResponse.getIndex());
            listener.onResponse(getResponse);
        };
    }

    /**
     * Wraps the completion of an UPDATE operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed UpdateResponse or any errors
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link OpenSearchStatusException} and {@link CompletionException}.
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    @SafeVarargs
    public static BiConsumer<UpdateDataObjectResponse, Throwable> wrapUpdateCompletion(
        ActionListener<UpdateResponse> listener,
        Class<? extends Throwable>... exceptionTypesToUnwrap
    ) {
        return (r, throwable) -> {
            if (throwable != null) {
                handleThrowable(listener, throwable, exceptionTypesToUnwrap);
                return;
            }
            UpdateResponse updateResponse = r.updateResponse();
            if (updateResponse == null) {
                handleParseFailure(listener, "update");
                return;
            }
            log.info("Update success for {} in {}", updateResponse.getId(), updateResponse.getIndex());
            listener.onResponse(updateResponse);
        };

    }

    /**
     * Wraps the completion of a DELETE operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed DeleteResponse or any errors
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link OpenSearchStatusException} and {@link CompletionException}.
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    @SafeVarargs
    public static BiConsumer<DeleteDataObjectResponse, Throwable> wrapDeleteCompletion(
        ActionListener<DeleteResponse> listener,
        Class<? extends Throwable>... exceptionTypesToUnwrap
    ) {
        return (r, throwable) -> {
            if (throwable != null) {
                handleThrowable(listener, throwable, exceptionTypesToUnwrap);
                return;
            }
            DeleteResponse deleteResponse = r.deleteResponse();
            if (deleteResponse == null) {
                handleParseFailure(listener, "delete");
                return;
            }
            log.info("Deleate success for {} in {}", deleteResponse.getId(), deleteResponse.getIndex());
            listener.onResponse(deleteResponse);
        };
    }

    /**
     * Wraps the completion of a BULK operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed BulkResponse or any errors
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link OpenSearchStatusException} and {@link CompletionException}.
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    @SafeVarargs
    public static BiConsumer<BulkDataObjectResponse, Throwable> wrapBulkCompletion(
        ActionListener<BulkResponse> listener,
        Class<? extends Throwable>... exceptionTypesToUnwrap
    ) {
        return (r, throwable) -> {
            if (throwable != null) {
                handleThrowable(listener, throwable, exceptionTypesToUnwrap);
                return;
            }
            BulkResponse bulkResponse = r.bulkResponse();
            if (bulkResponse == null) {
                handleParseFailure(listener, "bulk");
                return;
            }
            log.info("Bulk complete for {} items. {} failures.", bulkResponse.getItems().length, countBulkFailures(bulkResponse));
            listener.onResponse(bulkResponse);
        };
    }

    private static long countBulkFailures(BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            return Arrays.stream(bulkResponse.getItems()).filter(BulkItemResponse::isFailed).count();
        }
        return 0;
    }

    /**
     * Wraps the completion of a SEARCH operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed SearchResponse or any errors
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link OpenSearchStatusException} and {@link CompletionException}.
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    @SafeVarargs
    public static BiConsumer<SearchDataObjectResponse, Throwable> wrapSearchCompletion(
        ActionListener<SearchResponse> listener,
        Class<? extends Throwable>... exceptionTypesToUnwrap
    ) {
        return (r, throwable) -> {
            if (throwable != null) {
                handleThrowable(listener, throwable, exceptionTypesToUnwrap);
                return;
            }
            SearchResponse searchResponse = r.searchResponse();
            if (searchResponse == null) {
                handleParseFailure(listener, "search");
                return;
            }
            log.info("Search complete. {}.", searchResponse.getHits().getTotalHits());
            listener.onResponse(searchResponse);
        };
    }

    /**
     * Create a parser from a {@link ToXContent} object
     * @param obj The object to convert to a parser
     * @return the parser
     * @throws IOException on a parsing failure
     */
    public static XContentParser createParser(ToXContent obj) throws IOException {
        return createParser(Strings.toString(MediaTypeRegistry.JSON, obj));
    }

    /**
     * Create a parser from a JSON string
     * @param json The string to convert to a parser
     * @return the parser
     * @throws IOException on a parsing failure
     */
    public static XContentParser createParser(String json) throws IOException {
        return jsonXContent.createParser(DEFAULT_XCONTENT_REGISTRY, DeprecationHandler.IGNORE_DEPRECATIONS, json);
    }

    private static void handleParseFailure(ActionListener<?> listener, String operation) {
        listener.onFailure(new OpenSearchStatusException("Failed to parse " + operation + " response", INTERNAL_SERVER_ERROR));
    }

    @SafeVarargs
    private static void handleThrowable(
        ActionListener<?> listener,
        Throwable throwable,
        Class<? extends Throwable>... exceptionTypesToUnwrap
    ) {
        Exception exception = exceptionTypesToUnwrap.length > 0
            ? unwrapAndConvertToException(throwable, exceptionTypesToUnwrap)
            : unwrapAndConvertToException(throwable, OpenSearchStatusException.class, CompletionException.class);
        listener.onFailure(exception);
    }

    /**
     * Unwraps the cause of a {@link CompletionException}. If the cause is an {@link Exception}, rethrows the exception.
     * Otherwise wraps it in an {@link OpenSearchException}. Properly re-interrupts the thread on {@link InterruptedException}.
     * @param throwable a throwable.
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link CompletionException}.
     * @return the cause of the completion exception or the throwable, directly if an {@link Exception} or wrapped in an OpenSearchException otherwise.
     */
    @SafeVarargs
    public static Exception unwrapAndConvertToException(Throwable throwable, Class<? extends Throwable>... exceptionTypesToUnwrap) {
        // Unwrap specified exception types or pass through other exceptions
        List<Class<? extends Throwable>> unwrapTypes = (exceptionTypesToUnwrap.length > 0)
            ? Arrays.asList(exceptionTypesToUnwrap)
            : List.of(CompletionException.class);

        Throwable cause = throwable;
        while (cause != null && unwrapTypes.contains(cause.getClass()) && cause.getCause() != null) {
            cause = cause.getCause();
        }

        // Double-unwrap checked exceptions wrapped in ExecutionException
        cause = getRethrownExecutionExceptionRootCause(cause);
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return new OpenSearchException(cause);
    }

    /**
     * Get the original exception of an {@link UncategorizedExecutionException} with two levels of cause nesting.
     * Intended to recreate the root cause of an exception thrown by {@link ActionFuture#actionGet}, which was handled by {@link FutureUtils#rethrowExecutionException}.
     * @param throwable a throwable with possibly nested causes
     * @return the root cause of an ExecutionException if it was not a RuntimeException, otherwise the original exception
     */
    public static Throwable getRethrownExecutionExceptionRootCause(Throwable throwable) {
        if (throwable instanceof UncategorizedExecutionException && throwable.getCause() instanceof ExecutionException) {
            return throwable.getCause().getCause();
        }
        return throwable;
    }

    /**
     * If an internal variable is an enum represented by all upper case, the Remote client may have it mapped in lower case. This method lowercases these enum values
     * @param field The JSON field to lowercase the value
     * @param json The full JSON to process
     * @return The JSON with the value lowercased
     * @deprecated No longer required with OpenSearch Java Client v3
     */
    @Deprecated
    public static String lowerCaseEnumValues(String field, String json) {
        if (field == null) {
            return json;
        }
        if (json == null) {
            return null;
        }
        // Use a matcher to find and replace the field value in lowercase
        Matcher matcher = Pattern.compile("(\"" + Pattern.quote(field) + "\"):(\"[A-Z_]+\")").matcher(json);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, matcher.group(1) + ":" + matcher.group(2).toLowerCase(Locale.ROOT));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static NamedXContentRegistry createDefaultXContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(getDefaultNamedXContents());
        return new NamedXContentRegistry(entries);
    }

    private static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();

        Map<String, ContextParser<Object, ? extends QueryBuilder>> queryMap = Map.ofEntries(
            Map.entry(BoolQueryBuilder.NAME, (p, c) -> BoolQueryBuilder.fromXContent(p)),
            Map.entry(BoostingQueryBuilder.NAME, (p, c) -> BoostingQueryBuilder.fromXContent(p)),
            Map.entry(ConstantScoreQueryBuilder.NAME, (p, c) -> ConstantScoreQueryBuilder.fromXContent(p)),
            Map.entry(DisMaxQueryBuilder.NAME, (p, c) -> DisMaxQueryBuilder.fromXContent(p)),
            Map.entry(DistanceFeatureQueryBuilder.NAME, (p, c) -> DistanceFeatureQueryBuilder.fromXContent(p)),
            Map.entry(ExistsQueryBuilder.NAME, (p, c) -> ExistsQueryBuilder.fromXContent(p)),
            Map.entry(FieldMaskingSpanQueryBuilder.NAME, (p, c) -> FieldMaskingSpanQueryBuilder.fromXContent(p)),
            Map.entry(FuzzyQueryBuilder.NAME, (p, c) -> FuzzyQueryBuilder.fromXContent(p)),
            Map.entry(GeoBoundingBoxQueryBuilder.NAME, (p, c) -> GeoBoundingBoxQueryBuilder.fromXContent(p)),
            Map.entry(GeoDistanceQueryBuilder.NAME, (p, c) -> GeoDistanceQueryBuilder.fromXContent(p)),
            Map.entry(GeoPolygonQueryBuilder.NAME, (p, c) -> GeoPolygonQueryBuilder.fromXContent(p)),
            Map.entry(GeoShapeQueryBuilder.NAME, (p, c) -> GeoShapeQueryBuilder.fromXContent(p)),
            Map.entry(IdsQueryBuilder.NAME, (p, c) -> IdsQueryBuilder.fromXContent(p)),
            Map.entry(IntervalQueryBuilder.NAME, (p, c) -> IntervalQueryBuilder.fromXContent(p)),
            Map.entry(MatchAllQueryBuilder.NAME, (p, c) -> MatchAllQueryBuilder.fromXContent(p)),
            Map.entry(MatchBoolPrefixQueryBuilder.NAME, (p, c) -> MatchBoolPrefixQueryBuilder.fromXContent(p)),
            Map.entry(MatchNoneQueryBuilder.NAME, (p, c) -> MatchNoneQueryBuilder.fromXContent(p)),
            Map.entry(MatchPhrasePrefixQueryBuilder.NAME, (p, c) -> MatchPhrasePrefixQueryBuilder.fromXContent(p)),
            Map.entry(MatchPhraseQueryBuilder.NAME, (p, c) -> MatchPhraseQueryBuilder.fromXContent(p)),
            Map.entry(MatchQueryBuilder.NAME, (p, c) -> MatchQueryBuilder.fromXContent(p)),
            Map.entry(MoreLikeThisQueryBuilder.NAME, (p, c) -> MoreLikeThisQueryBuilder.fromXContent(p)),
            Map.entry(MultiMatchQueryBuilder.NAME, (p, c) -> MultiMatchQueryBuilder.fromXContent(p)),
            Map.entry(NestedQueryBuilder.NAME, (p, c) -> NestedQueryBuilder.fromXContent(p)),
            Map.entry(PrefixQueryBuilder.NAME, (p, c) -> PrefixQueryBuilder.fromXContent(p)),
            Map.entry(QueryStringQueryBuilder.NAME, (p, c) -> QueryStringQueryBuilder.fromXContent(p)),
            Map.entry(RangeQueryBuilder.NAME, (p, c) -> RangeQueryBuilder.fromXContent(p)),
            Map.entry(RegexpQueryBuilder.NAME, (p, c) -> RegexpQueryBuilder.fromXContent(p)),
            Map.entry(ScriptQueryBuilder.NAME, (p, c) -> ScriptQueryBuilder.fromXContent(p)),
            Map.entry(SimpleQueryStringBuilder.NAME, (p, c) -> SimpleQueryStringBuilder.fromXContent(p)),
            Map.entry(SpanContainingQueryBuilder.NAME, (p, c) -> SpanContainingQueryBuilder.fromXContent(p)),
            Map.entry(SpanFirstQueryBuilder.NAME, (p, c) -> SpanFirstQueryBuilder.fromXContent(p)),
            Map.entry(SpanNearQueryBuilder.NAME, (p, c) -> SpanNearQueryBuilder.fromXContent(p)),
            Map.entry(SpanNotQueryBuilder.NAME, (p, c) -> SpanNotQueryBuilder.fromXContent(p)),
            Map.entry(SpanOrQueryBuilder.NAME, (p, c) -> SpanOrQueryBuilder.fromXContent(p)),
            Map.entry(SpanTermQueryBuilder.NAME, (p, c) -> SpanTermQueryBuilder.fromXContent(p)),
            Map.entry(SpanWithinQueryBuilder.NAME, (p, c) -> SpanWithinQueryBuilder.fromXContent(p)),
            Map.entry(TemplateQueryBuilder.NAME, (p, c) -> TemplateQueryBuilder.fromXContent(p)),
            Map.entry(TermQueryBuilder.NAME, (p, c) -> TermQueryBuilder.fromXContent(p)),
            Map.entry(TermsQueryBuilder.NAME, (p, c) -> TermsQueryBuilder.fromXContent(p)),
            Map.entry(TermsSetQueryBuilder.NAME, (p, c) -> TermsSetQueryBuilder.fromXContent(p)),
            Map.entry(WildcardQueryBuilder.NAME, (p, c) -> WildcardQueryBuilder.fromXContent(p)),
            Map.entry(WrapperQueryBuilder.NAME, (p, c) -> WrapperQueryBuilder.fromXContent(p))
        );

        entries.addAll(
            queryMap.entrySet()
                .stream()
                .map(entry -> new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList())
        );

        Map<String, ContextParser<Object, ? extends Aggregation>> aggMap = Map.ofEntries(
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

        entries.addAll(
            aggMap.entrySet()
                .stream()
                .map(
                    (entry) -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField((String) entry.getKey()), entry.getValue())
                )
                .collect(Collectors.toList())
        );
        return entries;
    }
}
