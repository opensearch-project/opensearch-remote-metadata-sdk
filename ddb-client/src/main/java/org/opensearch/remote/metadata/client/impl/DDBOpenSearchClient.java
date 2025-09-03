/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.client.AbstractSdkClient;
import org.opensearch.remote.metadata.client.BulkDataObjectRequest;
import org.opensearch.remote.metadata.client.BulkDataObjectResponse;
import org.opensearch.remote.metadata.client.DataObjectRequest;
import org.opensearch.remote.metadata.client.DataObjectResponse;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectResponse;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.remote.metadata.client.UpdateDataObjectResponse;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.remote.metadata.common.CommonValue.AWS_DYNAMO_DB;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_GLOBAL_RESOURCE_CACHE_REFRESH_INTERVAL_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_GLOBAL_TENANT_ID_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.VALID_AWS_OPENSEARCH_SERVICE_NAMES;

/**
 * DDB implementation of {@link SdkClient}. DDB table name will be mapped to index name.
 *
 */
public class DDBOpenSearchClient extends AbstractSdkClient {
    private static final Logger log = LogManager.getLogger(RemoteClusterIndicesClient.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Long DEFAULT_SEQUENCE_NUMBER = 0L;
    private static final Long DEFAULT_PRIMARY_TERM = 1L;
    private static final String RANGE_KEY = "_id";
    private static final String HASH_KEY = "_tenant_id";

    private static final String SOURCE = "_source";
    private static final String SEQ_NO_KEY = "_seq_no";

    // TENANT_ID hash key requires non-null value
    private static final String DEFAULT_TENANT = "DEFAULT_TENANT";

    private DynamoDbAsyncClient dynamoDbAsyncClient;
    private AOSOpenSearchClient aosOpenSearchClient;
    private Scheduler.Cancellable globalResourcesCacheScheduler;

    public static String globalTenantId;
    private static final Map<String, Map<String, AttributeValue>> GLOBAL_RESOURCES_CACHE = new ConcurrentHashMap<>();
    private static TimeValue CACHE_REFRESH_INTERVAL;
    private static final TimeValue DEFAULT_CACHE_REFRESH_INTERVAL = TimeValue.timeValueMinutes(5);
    private static final TimeValue MIN_CACHE_REFRESH_INTERVAL = TimeValue.timeValueMinutes(1);


    @Override
    public boolean supportsMetadataType(String metadataType) {
        return AWS_DYNAMO_DB.equals(metadataType);
    }

    @Override
    public void initialize(Map<String, String> metadataSettings) {
        super.initialize(metadataSettings);
        validateAwsParams(remoteMetadataType, remoteMetadataEndpoint, region, serviceName);

        this.dynamoDbAsyncClient = createDynamoDbAsyncClient(region);
        this.aosOpenSearchClient = new AOSOpenSearchClient();
        this.aosOpenSearchClient.initialize(metadataSettings);
        globalTenantId = metadataSettings.get(REMOTE_METADATA_GLOBAL_TENANT_ID_KEY);
        CACHE_REFRESH_INTERVAL = parseCacheRefreshInterval(
                metadataSettings.get(REMOTE_METADATA_GLOBAL_RESOURCE_CACHE_REFRESH_INTERVAL_KEY)
        );
        if (globalTenantId != null) {
            cacheGlobalResources();
            // Only start scheduler if not disabled
            if (!TimeValue.MINUS_ONE.equals(CACHE_REFRESH_INTERVAL)) {
                startGlobalResourcesCacheScheduler();
            }
        }
    }

    /**
     * This method is to check if a resource is global based on index/table name and resource id.
     * @param index The index/table name.
     * @param id The resource id.
     * @return If the resource global one.
     */
    @Override
    public CompletionStage<Boolean> isGlobalResource(String index, String id) {
        if (globalTenantId == null) {
            return CompletableFuture.completedFuture(false);
        }
        
        GetItemRequest getItemRequest = GetItemRequest.builder()
            .tableName(index)
            .key(Map.of(
                HASH_KEY, AttributeValue.builder().s(globalTenantId).build(),
                RANGE_KEY, AttributeValue.builder().s(id).build()
            ))
            .consistentRead(true)
            .build();
            
        return doPrivileged(() -> dynamoDbAsyncClient.getItem(getItemRequest)
            .thenApply(getItemResponse -> {
                if (getItemResponse == null || getItemResponse.item() == null || getItemResponse.item().isEmpty()) {
                    return false;
                }
                
                AttributeValue tenantIdAttr = getItemResponse.item().get(HASH_KEY);
                return tenantIdAttr != null && globalTenantId.equals(tenantIdAttr.s());
            })
            .exceptionally(e -> {
                log.error("Error checking if resource is global for index: {} id: {}", index, id, e);
                return false;
            })
        );
    }

    /**
     * Empty constructor for SPI
     */
    public DDBOpenSearchClient() {}

    /**
     * Package private constructor for testing
     *
     * @param dynamoDbAsyncClient AWS DDB async client to perform CRUD operations on a DDB table.
     * @param aosOpenSearchClient Remote opensearch client to perform search operations. Documents written to DDB
     *                                  needs to be synced offline with remote opensearch.
     * @param tenantIdField the field name for the tenant id
     */
    DDBOpenSearchClient(DynamoDbAsyncClient dynamoDbAsyncClient, AOSOpenSearchClient aosOpenSearchClient, String tenantIdField) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.aosOpenSearchClient = aosOpenSearchClient;
        this.tenantIdField = tenantIdField;
    }

    /**
     * Package private constructor for testing with ThreadPool
     */
    DDBOpenSearchClient(
        DynamoDbAsyncClient dynamoDbAsyncClient,
        AOSOpenSearchClient aosOpenSearchClient,
        String tenantIdField,
        ThreadPool tp
    ) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.aosOpenSearchClient = aosOpenSearchClient;
        this.tenantIdField = tenantIdField;
        this.threadPool = tp;
    }

    private void cacheGlobalResources() {
        this.dynamoDbAsyncClient.listTables().thenAccept(tables -> {
            Map<String, CompletableFuture<DescribeTableResponse>> describeTableCompletableFutureMap = getDescribeTableCompletableFutureMap(
                tables
            );
            CompletableFuture.allOf(describeTableCompletableFutureMap.values().toArray(new CompletableFuture[0])).exceptionally(ex -> {
                log.error("Failed to describe DDB tables", ex);
                return null;
            }).thenAccept(y -> {
                List<String> potentialGlobalResourceTables = new ArrayList<>();
                for (Map.Entry<String, CompletableFuture<DescribeTableResponse>> responseFuture : describeTableCompletableFutureMap
                    .entrySet()) {
                    DescribeTableResponse describeTableResponse = responseFuture.getValue().join();
                    if (describeTableResponse.table().hasKeySchema()
                        && describeTableResponse.table().keySchema().stream().anyMatch(kse -> HASH_KEY.equals(kse.attributeName()))) {
                        potentialGlobalResourceTables.add(describeTableResponse.table().tableName());
                    }
                }
                if (!potentialGlobalResourceTables.isEmpty()) {
                    log.info("Found potential global resource tables: {}", potentialGlobalResourceTables);
                    Map<String, CompletableFuture<QueryResponse>> fetchGlobalResourcesCompletableFutureMap = potentialGlobalResourceTables
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), this::fetchGlobalResources));
                    CompletableFuture.allOf(fetchGlobalResourcesCompletableFutureMap.values().toArray(new CompletableFuture[0]))
                        .exceptionally(ex -> {
                            log.error("Failed to execute DDB query", ex);
                            return null;
                        })
                        .thenAccept(z -> {
                            for (Map.Entry<
                                String,
                                CompletableFuture<QueryResponse>> responseFuture : fetchGlobalResourcesCompletableFutureMap.entrySet()) {
                                QueryResponse queryResponse = responseFuture.getValue().join();
                                queryResponse.items().forEach(item -> {
                                    String id = item.get(RANGE_KEY).s();
                                    GLOBAL_RESOURCES_CACHE.put(buildGlobalCacheKey(responseFuture.getKey(), id), item);
                                });
                            }
                        });
                }
            });
        }).exceptionally((throwable -> {
            log.error("Failed to list tables!", throwable);
            return null;
        }));
    }

    private Map<String, CompletableFuture<DescribeTableResponse>> getDescribeTableCompletableFutureMap(ListTablesResponse x) {
        List<String> tableNames = x.tableNames();
        // Build all tables describe table query
        Map<String, CompletableFuture<DescribeTableResponse>> completableFutures = new HashMap<>();
        tableNames.forEach(y -> {
            DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(y).build();
            completableFutures.put(y, dynamoDbAsyncClient.describeTable(describeTableRequest));
        });
        return completableFutures;
    }

    private CompletableFuture<QueryResponse> fetchGlobalResources(String tableName) {
        Map<String, AttributeValue> attributeValueMap = ImmutableMap.of(":hash_key", AttributeValue.builder().s(globalTenantId).build());
        QueryRequest request = QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("#tid = " + ":hash_key")
            .expressionAttributeNames(ImmutableMap.of("#tid", "_tenant_id"))
            .expressionAttributeValues(attributeValueMap)
            .build();
        return dynamoDbAsyncClient.query(request);
    }

    /**
     * DDB implementation to write data objects to DDB table. Tenant ID will be used as hash key and document ID will
     * be used as range key. If tenant ID is not defined a default tenant ID will be used. If document ID is not defined
     * a random UUID will be generated. Data object will be written as a nested DDB attribute.
     *
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<PutDataObjectResponse> putDataObjectAsync(
        PutDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        final String id = shouldUseId(request.id()) ? request.id() : UUID.randomUUID().toString();
        // Validate parameters and data object body
        try (XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(request.index()).opType(request.overwriteIfExists() ? OpType.INDEX : OpType.CREATE)
                .source(request.dataObject().toXContent(sourceBuilder, ToXContent.EMPTY_PARAMS));
            indexRequest.id(id);
            ActionRequestValidationException validationException = indexRequest.validate();
            if (validationException != null) {
                throw new OpenSearchStatusException(validationException.getMessage(), RestStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            throw new OpenSearchStatusException("Request body validation failed.", RestStatus.BAD_REQUEST, e);
        }
        final String tenantId = request.tenantId() != null ? request.tenantId() : DEFAULT_TENANT;
        validateGlobalTenantId(globalTenantId, tenantId);
        final String tableName = request.index();
        final GetItemRequest getItemRequest = buildGetItemRequest(tenantId, id, request.index());

        return doPrivileged(() -> dynamoDbAsyncClient.getItem(getItemRequest).thenCompose(getItemResponse -> {
            try {
                // Fail fast if item exists to save an attempted conditional write
                if (!request.overwriteIfExists()
                    && getItemResponse != null
                    && getItemResponse.item() != null
                    && !getItemResponse.item().isEmpty()) {
                    throw new OpenSearchStatusException("Existing data object for ID: " + request.id(), RestStatus.CONFLICT);
                }

                Long sequenceNumber = initOrIncrementSeqNo(getItemResponse);
                String source = Strings.toString(MediaTypeRegistry.JSON, request.dataObject());
                JsonNode jsonNode = OBJECT_MAPPER.readTree(source);
                Map<String, AttributeValue> sourceMap = DDBJsonTransformer.convertJsonObjectToDDBAttributeMap(jsonNode);
                if (request.tenantId() != null) {
                    sourceMap.put(this.tenantIdField, AttributeValue.builder().s(tenantId).build());
                }
                Map<String, AttributeValue> item = new HashMap<>();
                item.put(HASH_KEY, AttributeValue.builder().s(tenantId).build());
                item.put(RANGE_KEY, AttributeValue.builder().s(id).build());
                item.put(SOURCE, AttributeValue.builder().m(sourceMap).build());
                item.put(SEQ_NO_KEY, AttributeValue.builder().n(sequenceNumber.toString()).build());
                PutItemRequest.Builder builder = PutItemRequest.builder().tableName(tableName).item(item);

                // Protect against race condition if another thread just created this
                if (!request.overwriteIfExists()) {
                    // CREATE operation - check item doesn't exist
                    builder.conditionExpression("attribute_not_exists(#hk) AND attribute_not_exists(#rk)")
                        .expressionAttributeNames(Map.of("#hk", HASH_KEY, "#rk", RANGE_KEY));
                } else if (request.ifSeqNo() != null) {
                    // INDEX operation with version check
                    builder.conditionExpression("#seqNo = :seqNo")
                        .expressionAttributeNames(Map.of("#seqNo", SEQ_NO_KEY))
                        .expressionAttributeValues(Map.of(":seqNo", AttributeValue.builder().n(Long.toString(request.ifSeqNo())).build()));
                }

                final PutItemRequest putItemRequest = builder.build();

                return dynamoDbAsyncClient.putItem(putItemRequest).thenApply(putItemResponse -> {
                    try {
                        String simulatedIndexResponse = simulateOpenSearchResponse(
                            request.index(),
                            id,
                            source,
                            sequenceNumber,
                            Map.of("result", "created")
                        );
                        return PutDataObjectResponse.builder().id(id).parser(SdkClientUtils.createParser(simulatedIndexResponse)).build();
                    } catch (IOException e) {
                        throw new OpenSearchStatusException("Failed to create parser for response", RestStatus.INTERNAL_SERVER_ERROR, e);
                    }
                })
                    // Thrown if overwriteIfExists is false
                    .exceptionally(e -> {
                        if (e.getCause() instanceof ConditionalCheckFailedException) {
                            String message = request.overwriteIfExists()
                                ? "Document version conflict for ID: " + request.id()
                                : "Concurrent write detected for ID: " + request.id();
                            throw new OpenSearchStatusException(message, RestStatus.CONFLICT);
                        }
                        if (e instanceof RuntimeException) {
                            throw (RuntimeException) e;
                        }
                        throw new CompletionException(e);
                    });
            } catch (IOException e) {
                throw new OpenSearchStatusException("Failed to parse data object " + request.id(), RestStatus.BAD_REQUEST, e);
            }
        }));
    }

    /**
     * Fetches data document from DDB. Default tenant ID will be used if tenant ID is not specified.
     *
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<GetDataObjectResponse> getDataObjectAsync(
        GetDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        if (globalTenantId == null) {
            final GetItemRequest getItemRequest = buildGetItemRequest(request.tenantId(), request.id(), request.index());
            return fetchDataFromDynamoDB(getItemRequest, request);
        } else {
            CompletionStage<GetDataObjectResponse> getDataObjectFromCache = getGlobalResourceDataFromCache(request);
            if (getDataObjectFromCache != null) {
                return getDataObjectFromCache;
            }
            final GetItemRequest getItemRequest = buildGetItemRequest(request.tenantId(), request.id(), request.index());
            CompletionStage<GetDataObjectResponse> getDataFromDynamoDB = fetchDataFromDynamoDB(getItemRequest, request);
            return getDataFromDynamoDB.thenCompose(response -> {
                // Check if the response has actual data
                if (response != null && response.source() != null && !response.source().isEmpty()) {
                    // If we have valid data, return it
                    return CompletableFuture.completedFuture(response);
                }

                // If we don't have valid data, proceed with the global tenant request
                final GetItemRequest getGlobalItemRequest = buildGetItemRequest(globalTenantId, request.id(), request.index());
                return fetchDataFromDynamoDB(getGlobalItemRequest, request);
            });
        }
    }

    /**
     * Fetches data from DynamoDB and transforms it into a GetDataObjectResponse.
     *
     * @param getItemRequest The DynamoDB GetItem request
     * @param originalRequest The original GetDataObject request
     * @return A CompletionStage with the GetDataObjectResponse
     */
    private CompletionStage<GetDataObjectResponse> fetchDataFromDynamoDB(
        GetItemRequest getItemRequest,
        GetDataObjectRequest originalRequest
    ) {
        return doPrivileged(() -> dynamoDbAsyncClient.getItem(getItemRequest)).thenApply(getItemResponse -> {
            try {
                ObjectNode sourceObject;
                boolean found;
                String sequenceNumberString = null;
                if (getItemResponse == null || getItemResponse.item() == null || getItemResponse.item().isEmpty()) {
                    found = false;
                    sourceObject = null;
                } else {
                    found = true;
                    sourceObject = DDBJsonTransformer.convertDDBAttributeValueMapToObjectNode(getItemResponse.item().get(SOURCE).m());
                    if (getItemResponse.item().containsKey(SEQ_NO_KEY)) {
                        sequenceNumberString = getItemResponse.item().get(SEQ_NO_KEY).n();
                    }
                }
                final String source = OBJECT_MAPPER.writeValueAsString(sourceObject);
                final Long sequenceNumber = sequenceNumberString == null || sequenceNumberString.isEmpty()
                    ? null
                    : Long.parseLong(sequenceNumberString);
                String simulatedGetResponse = simulateOpenSearchResponse(
                    originalRequest.index(),
                    originalRequest.id(),
                    source,
                    sequenceNumber,
                    Map.of("found", found)
                );
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    simulatedGetResponse
                );
                // This would consume parser content so we need to create a new parser for the map
                Map<String, Object> sourceAsMap = GetResponse.fromXContent(
                    JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        simulatedGetResponse
                    )
                ).getSourceAsMap();
                return GetDataObjectResponse.builder().id(originalRequest.id()).parser(parser).source(sourceAsMap).build();
            } catch (IOException e) {
                // Rethrow unchecked exception on XContent parsing error
                throw new OpenSearchStatusException("Failed to parse response", RestStatus.INTERNAL_SERVER_ERROR);
            }
        });
    }

    private CompletionStage<GetDataObjectResponse> getGlobalResourceDataFromCache(GetDataObjectRequest request) {
        String checkingKey = buildGlobalCacheKey(request.index(), request.id());
        if (GLOBAL_RESOURCES_CACHE.containsKey(checkingKey)) {
            Map<String, AttributeValue> item = GLOBAL_RESOURCES_CACHE.get(checkingKey);
            // Replace the tenant id in the global resource response to actual tenant id to bypass the validation of the resources:
            // e.g.:
            // https://github.com/opensearch-project/ml-commons/blob/main/ml-algorithms/src/main/java/org/opensearch/ml/engine/algorithms/agent/MLAgentExecutor.java#L206
            Long seqNo = Optional.ofNullable(item.get(SEQ_NO_KEY)).map(AttributeValue::n).map(Long::parseLong).orElse(null);
            ObjectNode sourceObject = null;
            if (item.containsKey(SOURCE)) {
                sourceObject = DDBJsonTransformer.convertDDBAttributeValueMapToObjectNode(item.get(SOURCE).m());
            }
            if (sourceObject == null) {
                log.error("Empty global resource in cache!");
                throw new OpenSearchStatusException("Empty global resource in cache!", RestStatus.INTERNAL_SERVER_ERROR);
            } else {
                try {
                    sourceObject.put(TENANT_ID_FIELD_KEY, request.tenantId());
                    String source = OBJECT_MAPPER.writeValueAsString(sourceObject);
                    String simulatedGetResponse = simulateOpenSearchResponse(
                        request.index(),
                        request.id(),
                        source,
                        seqNo,
                        Map.of("found", true)
                    );
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        simulatedGetResponse
                    );
                    Map<String, Object> sourceAsMap = GetResponse.fromXContent(
                        JsonXContent.jsonXContent.createParser(
                            NamedXContentRegistry.EMPTY,
                            LoggingDeprecationHandler.INSTANCE,
                            simulatedGetResponse
                        )
                    ).getSourceAsMap();
                    return CompletableFuture.completedFuture(
                        GetDataObjectResponse.builder().id(request.id()).parser(parser).source(sourceAsMap).build()
                    );
                } catch (IOException e) {
                    throw new OpenSearchStatusException("Failed to parse cached global response", RestStatus.INTERNAL_SERVER_ERROR, e);
                }
            }
        }
        return null;
    }

    /**
     * Makes use of DDB update request to update data object.
     *
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<UpdateDataObjectResponse> updateDataObjectAsync(
        UpdateDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        // Validate parameters and data object body
        try (XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()) {
            UpdateRequest updateRequest = new UpdateRequest(request.index(), request.id()).doc(
                request.dataObject().toXContent(sourceBuilder, ToXContent.EMPTY_PARAMS)
            );

            if (request.ifSeqNo() != null) {
                updateRequest.setIfSeqNo(request.ifSeqNo());
            }
            if (request.ifPrimaryTerm() != null) {
                updateRequest.setIfPrimaryTerm(request.ifPrimaryTerm());
            }
            if (request.retryOnConflict() > 0) {
                updateRequest.retryOnConflict(request.retryOnConflict());
            }
            ActionRequestValidationException validationException = updateRequest.validate();
            if (validationException != null) {
                throw new OpenSearchStatusException(validationException.getMessage(), RestStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            throw new OpenSearchStatusException("Request body validation failed.", RestStatus.BAD_REQUEST, e);
        }
        final String tenantId = request.tenantId() != null ? request.tenantId() : DEFAULT_TENANT;
        validateGlobalTenantId(globalTenantId, tenantId);
        return doPrivileged(() -> {
            try {
                String source = Strings.toString(MediaTypeRegistry.JSON, request.dataObject());
                JsonNode jsonNode = OBJECT_MAPPER.readTree(source);

                return updateItemWithRetryOnConflict(tenantId, jsonNode, request).thenApply(sequenceNumber -> {
                    try {
                        String simulatedUpdateResponse = simulateOpenSearchResponse(
                            request.index(),
                            request.id(),
                            source,
                            sequenceNumber,
                            Map.of("result", "updated")
                        );
                        return UpdateDataObjectResponse.builder()
                            .id(request.id())
                            .parser(SdkClientUtils.createParser(simulatedUpdateResponse))
                            .build();
                    } catch (IOException e) {
                        throw new OpenSearchStatusException("Parsing error creating update response", RestStatus.INTERNAL_SERVER_ERROR, e);
                    }
                });
            } catch (IOException e) {
                log.error("Error updating {} in {}: {}", request.id(), request.index(), e.getMessage(), e);
                // Rethrow unchecked exception on update IOException
                throw new OpenSearchStatusException(
                    "Parsing error updating data object " + request.id() + " in index " + request.index(),
                    RestStatus.BAD_REQUEST
                );
            }
        });
    }

    private CompletionStage<Long> updateItemWithRetryOnConflict(String tenantId, JsonNode jsonNode, UpdateDataObjectRequest request) {
        Map<String, AttributeValue> updateItem = DDBJsonTransformer.convertJsonObjectToDDBAttributeMap(jsonNode);
        updateItem.remove(this.tenantIdField);
        updateItem.remove(RANGE_KEY);
        Map<String, AttributeValue> updateKey = new HashMap<>();
        updateKey.put(HASH_KEY, AttributeValue.builder().s(tenantId).build());
        updateKey.put(RANGE_KEY, AttributeValue.builder().s(request.id()).build());
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#seqNo", SEQ_NO_KEY);
        expressionAttributeNames.put("#source", SOURCE);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":incr", AttributeValue.builder().n("1").build());

        return retryUpdate(request, updateKey, updateItem, expressionAttributeNames, expressionAttributeValues, request.retryOnConflict());
    }

    private CompletionStage<Long> retryUpdate(
        UpdateDataObjectRequest request,
        Map<String, AttributeValue> updateKey,
        Map<String, AttributeValue> updateItem,
        Map<String, String> expressionAttributeNames,
        Map<String, AttributeValue> expressionAttributeValues,
        int retriesRemaining
    ) {
        return dynamoDbAsyncClient.getItem(GetItemRequest.builder().tableName(request.index()).key(updateKey).build())
            .thenCompose(currentItem -> {
                // Fetch current item and extract data object
                Map<String, AttributeValue> dataObject = new HashMap<>(currentItem.item().get(SOURCE).m());
                // Update existing with changes
                dataObject.putAll(updateItem);
                expressionAttributeValues.put(":source", AttributeValue.builder().m(dataObject).build());
                // Use seqNo from the object we got to make sure we're updating the same thing
                if (request.ifSeqNo() != null) {
                    expressionAttributeValues.put(":currentSeqNo", AttributeValue.builder().n(Long.toString(request.ifSeqNo())).build());
                } else {
                    expressionAttributeValues.put(":currentSeqNo", currentItem.item().get(SEQ_NO_KEY));
                }
                UpdateItemRequest.Builder updateItemRequestBuilder = UpdateItemRequest.builder().tableName(request.index()).key(updateKey);
                updateItemRequestBuilder.updateExpression("SET #seqNo = #seqNo + :incr, #source = :source ");
                updateItemRequestBuilder.conditionExpression("#seqNo = :currentSeqNo");
                updateItemRequestBuilder.expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues);
                // Needed to get SEQ_NO_KEY value for the response
                updateItemRequestBuilder.returnValues("UPDATED_NEW");

                UpdateItemRequest updateItemRequest = updateItemRequestBuilder.build();

                return dynamoDbAsyncClient.updateItem(updateItemRequest).thenApply(updateItemResponse -> {
                    if (updateItemResponse != null
                        && updateItemResponse.attributes() != null
                        && updateItemResponse.attributes().containsKey(SEQ_NO_KEY)) {
                        return Long.parseLong(updateItemResponse.attributes().get(SEQ_NO_KEY).n());
                    }
                    return null;
                }).exceptionally(e -> {
                    if (e.getCause() instanceof ConditionalCheckFailedException) {
                        if (retriesRemaining > 0) {
                            return retryUpdate(
                                request,
                                updateKey,
                                updateItem,
                                expressionAttributeNames,
                                expressionAttributeValues,
                                retriesRemaining - 1
                            ).toCompletableFuture().join();
                        } else {
                            String message = "Document version conflict updating " + request.id() + " in index " + request.index();
                            log.error(message + ": {}", e.getMessage(), e);
                            throw new OpenSearchStatusException(message, RestStatus.CONFLICT);
                        }
                    }
                    throw new CompletionException(e);
                });
            });
    }

    /**
     * Deletes data document from DDB. Default tenant ID will be used if tenant ID is not specified.
     *
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<DeleteDataObjectResponse> deleteDataObjectAsync(
        DeleteDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        final String tenantId = request.tenantId() != null ? request.tenantId() : DEFAULT_TENANT;
        validateGlobalTenantId(globalTenantId, tenantId);
        DeleteItemRequest.Builder builder = DeleteItemRequest.builder()
            .tableName(request.index())
            .key(
                Map.ofEntries(
                    Map.entry(HASH_KEY, AttributeValue.builder().s(tenantId).build()),
                    Map.entry(RANGE_KEY, AttributeValue.builder().s(request.id()).build())
                )
            )
            // Needed to get SEQ_NO_KEY value for the response
            .returnValues("ALL_OLD");

        if (request.ifSeqNo() != null) {
            builder.conditionExpression("#seqNo = :seqNo")
                .expressionAttributeNames(Map.of("#seqNo", SEQ_NO_KEY))
                .expressionAttributeValues(Map.of(":seqNo", AttributeValue.builder().n(Long.toString(request.ifSeqNo())).build()));
        }

        final DeleteItemRequest deleteItemRequest = builder.build();
        return doPrivileged(() -> dynamoDbAsyncClient.deleteItem(deleteItemRequest).thenApply(deleteItemResponse -> {
            try {
                Long sequenceNumber = null;
                if (deleteItemResponse.attributes() != null && deleteItemResponse.attributes().containsKey(SEQ_NO_KEY)) {
                    sequenceNumber = Long.parseLong(deleteItemResponse.attributes().get(SEQ_NO_KEY).n()) + 1;
                }
                String simulatedDeleteResponse = simulateOpenSearchResponse(
                    request.index(),
                    request.id(),
                    null,
                    sequenceNumber,
                    Map.of("result", "deleted")
                );
                return DeleteDataObjectResponse.builder()
                    .id(request.id())
                    .parser(SdkClientUtils.createParser(simulatedDeleteResponse))
                    .build();
            } catch (IOException e) {
                // Rethrow unchecked exception on XContent parsing error
                throw new OpenSearchStatusException("Failed to parse response", RestStatus.INTERNAL_SERVER_ERROR);
            }
        }).exceptionally(e -> {
            if (e.getCause() instanceof ConditionalCheckFailedException) {
                String message = "Document version conflict deleting " + request.id() + " from index " + request.index();
                throw new OpenSearchStatusException(message, RestStatus.CONFLICT);
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new CompletionException(e);
        }));

    }

    @Override
    public CompletionStage<BulkDataObjectResponse> bulkDataObjectAsync(
        BulkDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        return doPrivileged(() -> {
            log.info("Performing {} bulk actions on table {}", request.requests().size(), request.getIndices());
            long startNanos = System.nanoTime();
            return processBulkRequestsAsync(request.requests(), 0, new ArrayList<>(), executor, isMultiTenancyEnabled).thenCompose(
                responses -> {
                    long endNanos = System.nanoTime();
                    long tookMillis = TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos);
                    log.info("Bulk action complete for {} items, took {} ms", responses.size(), tookMillis);
                    return buildBulkDataObjectResponse(responses, tookMillis);
                }
            );
        });
    }

    private CompletionStage<List<DataObjectResponse>> processBulkRequestsAsync(
        List<DataObjectRequest> requests,
        int index,
        List<DataObjectResponse> responses,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        if (index >= requests.size()) {
            return CompletableFuture.completedFuture(responses);
        }

        DataObjectRequest dataObjectRequest = requests.get(index);
        CompletionStage<? extends DataObjectResponse> futureResponse;

        if (dataObjectRequest instanceof PutDataObjectRequest) {
            futureResponse = putDataObjectAsync((PutDataObjectRequest) dataObjectRequest, executor, isMultiTenancyEnabled);
        } else if (dataObjectRequest instanceof UpdateDataObjectRequest) {
            futureResponse = updateDataObjectAsync((UpdateDataObjectRequest) dataObjectRequest, executor, isMultiTenancyEnabled);
        } else if (dataObjectRequest instanceof DeleteDataObjectRequest) {
            futureResponse = deleteDataObjectAsync((DeleteDataObjectRequest) dataObjectRequest, executor, isMultiTenancyEnabled);
        } else {
            futureResponse = CompletableFuture.failedFuture(
                new IllegalArgumentException("Unsupported request type: " + dataObjectRequest.getClass().getSimpleName())
            );
        }

        return futureResponse.handle((response, throwable) -> {
            if (throwable != null) {
                Exception cause = SdkClientUtils.unwrapAndConvertToException(throwable);
                RestStatus status = ExceptionsHelper.status(cause);
                if (dataObjectRequest instanceof PutDataObjectRequest) {
                    return new PutDataObjectResponse.Builder().index(dataObjectRequest.index())
                        .id(dataObjectRequest.id())
                        .failed(true)
                        .cause(cause)
                        .status(status)
                        .build();
                } else if (dataObjectRequest instanceof UpdateDataObjectRequest) {
                    return new UpdateDataObjectResponse.Builder().index(dataObjectRequest.index())
                        .id(dataObjectRequest.id())
                        .failed(true)
                        .cause(cause)
                        .status(status)
                        .build();
                } else if (dataObjectRequest instanceof DeleteDataObjectRequest) {
                    return new DeleteDataObjectResponse.Builder().index(dataObjectRequest.index())
                        .id(dataObjectRequest.id())
                        .failed(true)
                        .cause(cause)
                        .status(status)
                        .build();
                }
                log.error("Error in bulk operation for id {}: {}", dataObjectRequest.id(), throwable.getMessage(), throwable);
            }
            return response;
        }).thenCompose(response -> {
            responses.add(response);
            return processBulkRequestsAsync(requests, index + 1, responses, executor, isMultiTenancyEnabled);
        });
    }

    private CompletionStage<BulkDataObjectResponse> buildBulkDataObjectResponse(List<DataObjectResponse> responses, long tookMillis) {
        // Reconstruct BulkResponse to leverage its parser and hasFailed methods
        BulkItemResponse[] responseArray = new BulkItemResponse[responses.size()];
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            for (int id = 0; id < responses.size(); id++) {
                responseArray[id] = buildBulkItemResponse(responses, id);
            }
            BulkResponse br = new BulkResponse(responseArray, tookMillis);
            br.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return CompletableFuture.completedFuture(
                new BulkDataObjectResponse(
                    responses.toArray(new DataObjectResponse[0]),
                    tookMillis,
                    br.hasFailures(),
                    SdkClientUtils.createParser(builder.toString())
                )
            );
        } catch (IOException e) {
            // Rethrow unchecked exception on XContent parsing error
            return CompletableFuture.failedFuture(
                new OpenSearchStatusException("Failed to parse bulk response", RestStatus.INTERNAL_SERVER_ERROR)
            );
        }
    }

    private BulkItemResponse buildBulkItemResponse(List<DataObjectResponse> responses, int bulkId) throws IOException {
        DataObjectResponse response = responses.get(bulkId);
        OpType opType = null;
        if (response instanceof PutDataObjectResponse) {
            opType = OpType.INDEX;
        } else if (response instanceof UpdateDataObjectResponse) {
            opType = OpType.UPDATE;
        } else if (response instanceof DeleteDataObjectResponse) {
            opType = OpType.DELETE;
        }
        // If failed, parser is null, so shortcut response here
        if (response.isFailed()) {
            return new BulkItemResponse(bulkId, opType, new BulkItemResponse.Failure(response.index(), response.id(), response.cause()));
        }
        DocWriteResponse writeResponse = null;
        if (response instanceof PutDataObjectResponse) {
            writeResponse = IndexResponse.fromXContent(response.parser());
        } else if (response instanceof UpdateDataObjectResponse) {
            writeResponse = UpdateResponse.fromXContent(response.parser());
        } else if (response instanceof DeleteDataObjectResponse) {
            writeResponse = DeleteResponse.fromXContent(response.parser());
        }
        return new BulkItemResponse(bulkId, opType, writeResponse);
    }

    /**
     * DDB data needs to be synced with opensearch cluster. {@link RemoteClusterIndicesClient} will then be used to
     * search data in opensearch cluster.
     *
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<SearchDataObjectResponse> searchDataObjectAsync(
        SearchDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        List<String> indices = Arrays.stream(request.indices()).map(this::getIndexName).collect(Collectors.toList());

        SearchDataObjectRequest searchDataObjectRequest = new SearchDataObjectRequest(
            indices.toArray(new String[0]),
            request.tenantId(),
            request.searchSourceBuilder()
        );
        return this.aosOpenSearchClient.searchDataObjectAsync(searchDataObjectRequest, executor, isMultiTenancyEnabled);
    }

    private String getIndexName(String index) {
        // System index is not supported in remote index. Replacing '.' from index name.
        return (index.length() > 1 && index.charAt(0) == '.') ? index.substring(1) : index;
    }

    private GetItemRequest buildGetItemRequest(String requestTenantId, String documentId, String index) {
        final String tenantId = requestTenantId != null ? requestTenantId : DEFAULT_TENANT;
        return GetItemRequest.builder()
            .tableName(index)
            .key(
                Map.ofEntries(
                    Map.entry(HASH_KEY, AttributeValue.builder().s(tenantId).build()),
                    Map.entry(RANGE_KEY, AttributeValue.builder().s(documentId).build())
                )
            )
            .consistentRead(true)
            .build();
    }

    private Long initOrIncrementSeqNo(GetItemResponse getItemResponse) {
        Long sequenceNumber = DEFAULT_SEQUENCE_NUMBER;
        if (getItemResponse != null && getItemResponse.item() != null && getItemResponse.item().containsKey(SEQ_NO_KEY)) {
            sequenceNumber = Long.parseLong(getItemResponse.item().get(SEQ_NO_KEY).n()) + 1;
        }
        return sequenceNumber;
    }

    // package private for testing
    static String simulateOpenSearchResponse(
        String index,
        String id,
        String source,
        Long sequenceNumber,
        Map<String, Object> additionalFields
    ) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("_index", index);
        response.put("_id", id);
        if (sequenceNumber == null) {
            response.put("_primary_term", UNASSIGNED_PRIMARY_TERM);
            response.put("_seq_no", UNASSIGNED_SEQ_NO);
        } else {
            response.put("_primary_term", DEFAULT_PRIMARY_TERM);
            response.put("_seq_no", sequenceNumber);
        }
        response.put("_version", -1);
        response.put("_shards", new ShardInfo());
        response.putAll(additionalFields);
        if (source != null) {
            response.put("_source", mapper.readTree(source));
        }
        return mapper.writeValueAsString(response);
    }

    private static void validateAwsParams(String clientType, String remoteMetadataEndpoint, String region, String serviceName) {
        if (Strings.isNullOrEmpty(remoteMetadataEndpoint) || Strings.isNullOrEmpty(region)) {
            throw new OpenSearchException(clientType + " client requires a metadata endpoint and region.");
        }
        if (serviceName == null) {
            throw new OpenSearchException(clientType + " client requires a service name.");
        }
        if (!VALID_AWS_OPENSEARCH_SERVICE_NAMES.contains(serviceName)) {
            throw new OpenSearchException(clientType + " client only supports service names " + VALID_AWS_OPENSEARCH_SERVICE_NAMES);
        }
    }

    private static DynamoDbAsyncClient createDynamoDbAsyncClient(String region) {
        if (region == null) {
            throw new IllegalStateException("REGION environment variable needs to be set!");
        }
        return doPrivileged(
            () -> DynamoDbAsyncClient.builder()
                .httpClient(NettyNioAsyncHttpClient.builder().build())
                .region(Region.of(region))
                .credentialsProvider(createCredentialsProvider())
                .build()
        );
    }

    private static AwsCredentialsProvider createCredentialsProvider() {
        return AwsCredentialsProviderChain.builder()
            .addCredentialsProvider(EnvironmentVariableCredentialsProvider.create())
            .addCredentialsProvider(ContainerCredentialsProvider.builder().build())
            .addCredentialsProvider(InstanceProfileCredentialsProvider.create())
            .build();
    }

    private void startGlobalResourcesCacheScheduler() {
        if (threadPool != null && !TimeValue.MINUS_ONE.equals(CACHE_REFRESH_INTERVAL)) {
            log.info("Starting global resources cache scheduler with interval: {}", CACHE_REFRESH_INTERVAL);
            globalResourcesCacheScheduler = threadPool.scheduleWithFixedDelay(
                    this::cacheGlobalResources,
                    CACHE_REFRESH_INTERVAL,
                    ThreadPool.Names.GENERIC
            );
        } else if (TimeValue.MINUS_ONE.equals(CACHE_REFRESH_INTERVAL)) {
            log.info("Global resources cache refresh is disabled");
        } else {
            log.warn("ThreadPool not available, global resources cache scheduler not started");
        }
    }

    private void stopGlobalResourcesCacheScheduler() {
        if (globalResourcesCacheScheduler != null) {
            globalResourcesCacheScheduler.cancel();
            log.info("Stopped global resources cache scheduler");
        }
    }

    @Override
    public void close() throws Exception {
        stopGlobalResourcesCacheScheduler();
        if (dynamoDbAsyncClient != null) {
            dynamoDbAsyncClient.close();
        }
        if (aosOpenSearchClient != null) {
            aosOpenSearchClient.close();
        }
    }

    private static TimeValue parseCacheRefreshInterval(String value) {
        if (value == null || value.trim().isEmpty()) {
            return DEFAULT_CACHE_REFRESH_INTERVAL;
        }

        try {
            TimeValue parsed = TimeValue.parseTimeValue(value, "cache_refresh_interval");

            // Allow MINUS_ONE to disable cache refresh
            if (TimeValue.MINUS_ONE.equals(parsed)) {
                return TimeValue.MINUS_ONE;
            }

            // Enforce minimum interval to prevent abuse
            if (parsed.compareTo(MIN_CACHE_REFRESH_INTERVAL) < 0) {
                log.warn("Cache refresh interval {} is below minimum {}, using minimum", parsed, MIN_CACHE_REFRESH_INTERVAL);
                return MIN_CACHE_REFRESH_INTERVAL;
            }

            return parsed;
        } catch (Exception e) {
            log.warn("Invalid cache refresh interval '{}', using default {}: {}", value, DEFAULT_CACHE_REFRESH_INTERVAL, e.getMessage());
            return DEFAULT_CACHE_REFRESH_INTERVAL;
        }
    }
}
