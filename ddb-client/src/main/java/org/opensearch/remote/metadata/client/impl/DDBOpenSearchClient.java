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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.cryptography.dbencryptionsdk.dynamodb.itemencryptor.DynamoDbItemEncryptor;
import software.amazon.cryptography.dbencryptionsdk.dynamodb.itemencryptor.model.DecryptItemInput;
import software.amazon.cryptography.dbencryptionsdk.dynamodb.itemencryptor.model.DynamoDbItemEncryptorConfig;
import software.amazon.cryptography.dbencryptionsdk.dynamodb.itemencryptor.model.EncryptItemInput;
import software.amazon.cryptography.dbencryptionsdk.structuredencryption.model.CryptoAction;
import software.amazon.cryptography.materialproviders.IClientSupplier;
import software.amazon.cryptography.materialproviders.IKeyring;
import software.amazon.cryptography.materialproviders.MaterialProviders;
import software.amazon.cryptography.materialproviders.model.CreateAwsKmsMrkMultiKeyringInput;
import software.amazon.cryptography.materialproviders.model.GetClientInput;
import software.amazon.cryptography.materialproviders.model.MaterialProvidersConfig;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.remote.metadata.common.CommonValue.AWS_DYNAMO_DB;
import static org.opensearch.remote.metadata.common.CommonValue.VALID_AWS_OPENSEARCH_SERVICE_NAMES;
import static org.opensearch.remote.metadata.common.SdkClientUtils.createParser;
import static org.opensearch.secure_sm.AccessController.doPrivileged;

/**
 * DDB implementation of {@link SdkClient}. DDB table name will be mapped to index name.
 *
 */
public class DDBOpenSearchClient extends AbstractSdkClient {
    private static final Logger log = LogManager.getLogger(RemoteClusterIndicesClient.class);

    private static final Long DEFAULT_SEQUENCE_NUMBER = 0L;
    private static final Long DEFAULT_PRIMARY_TERM = 1L;
    private static final String RANGE_KEY = "_id";
    private static final String HASH_KEY = "_tenant_id";

    private static final String SOURCE = "_source";
    private static final String SEQ_NO_KEY = "_seq_no";

    private DynamoDbAsyncClient dynamoDbAsyncClient;
    private AOSOpenSearchClient aosOpenSearchClient;

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
     * Package private constructor for testing
     *
     * @param dynamoDbAsyncClient AWS DDB async client to perform CRUD operations on a DDB table.
     * @param aosOpenSearchClient Remote opensearch client to perform search operations. Documents written to DDB
     *                                  needs to be synced offline with remote opensearch.
     * @param metadataSettings metadata settings used in the class.
     */
    DDBOpenSearchClient(
        DynamoDbAsyncClient dynamoDbAsyncClient,
        AOSOpenSearchClient aosOpenSearchClient,
        Map<String, String> metadataSettings
    ) {
        super.initialize(metadataSettings);
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.aosOpenSearchClient = aosOpenSearchClient;
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
                if (Objects.nonNull(request.cmkRoleArn())) {
                    final DynamoDbItemEncryptor enc = getEncryptorForTable(tableName, request.cmkRoleArn(), request.assumeRoleArn());
                    item = enc.EncryptItem(EncryptItemInput.builder().plaintextItem(item).build()).encryptedItem();
                }

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
                        return PutDataObjectResponse.builder().id(id).parser(createParser(simulatedIndexResponse)).build();
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
     * 0. Tenant_id: Fetches data document from DDB. Default tenant ID will be used if tenant ID is not specified.
     * The fetching flow is:
     * 1. If global resource is not enabled, fetch data with tenant_id in step 0.
     * 2. If global resource enabled, fetch from cache and return if found.
     * 3. If not found, fetch with tenant_id in step 0, return if found.
     * 4. If not found, fetch with global tenant id and add to cache if found, then return result to user.
     *
     * {@inheritDoc}
     */
    @Override
    public CompletionStage<GetDataObjectResponse> getDataObjectAsync(
        GetDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        if (Boolean.FALSE.equals(isMultiTenancyEnabled) || Strings.isNullOrEmpty(globalTenantId)) {
            return innerGetDataObjectAsync(request, executor, isMultiTenancyEnabled);
        }
        // Try fetch from global cache.
        GetDataObjectResponse getDataObjectFromCache = getGlobalResourceDataFromCache(request);
        if (getDataObjectFromCache != null) {
            return CompletableFuture.completedFuture(getDataObjectFromCache);
        }
        // fetch resource with user tenant id.
        CompletionStage<GetDataObjectResponse> getDataFromDynamoDB = innerGetDataObjectAsync(request, executor, isMultiTenancyEnabled);
        return getDataFromDynamoDB.thenCompose(response -> {
            // return the document if it's exist under user tenant id.
            if (Optional.ofNullable(response).map(GetDataObjectResponse::getResponse).map(GetResponse::isExists).orElse(false)) {
                return CompletableFuture.completedFuture(response);
            }

            // Fetch with the global tenant id
            final GetDataObjectRequest requestWithGlobalTenantId = GetDataObjectRequest.builder()
                .tenantId(globalTenantId)
                .id(request.id())
                .index(request.index())
                .build();
            CompletionStage<GetDataObjectResponse> dataFetchedWithGlobalTenantId = innerGetDataObjectAsync(
                requestWithGlobalTenantId,
                executor,
                isMultiTenancyEnabled
            );
            return addToGlobalResourceCache(request, dataFetchedWithGlobalTenantId);
        });
    }

    /**
     * Fetches data from DynamoDB and transforms it into a GetDataObjectResponse.
     *
     * @param request The original GetDataObject request
     * @param executor the executor for the action
     * @param isMultiTenancyEnabled multi tenancy enabled flag.
     * @return A {@link CompletionStage} with the {@link GetDataObjectResponse}
     */
    protected CompletionStage<GetDataObjectResponse> innerGetDataObjectAsync(
        GetDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        final GetItemRequest getItemRequest = buildGetItemRequest(request.tenantId(), request.id(), request.index());
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
                    Map<String, AttributeValue> resultItems = getItemResponse.item();
                    if (nonNull(request.cmkRoleArn())) {
                        DynamoDbItemEncryptor dynamoDbItemEncryptor = getEncryptorForTable(
                            getItemRequest.tableName(),
                            request.cmkRoleArn(),
                            request.assumeRoleArn()
                        );
                        resultItems = dynamoDbItemEncryptor.DecryptItem(
                            DecryptItemInput.builder().encryptedItem(getItemResponse.item()).build()
                        ).plaintextItem();
                    }

                    sourceObject = DDBJsonTransformer.convertDDBAttributeValueMapToObjectNode(resultItems.get(SOURCE).m());
                    if (getItemResponse.item().containsKey(SEQ_NO_KEY)) {
                        sequenceNumberString = getItemResponse.item().get(SEQ_NO_KEY).n();
                    }
                }
                final String source = OBJECT_MAPPER.writeValueAsString(sourceObject);
                final Long sequenceNumber = sequenceNumberString == null || sequenceNumberString.isEmpty()
                    ? null
                    : Long.parseLong(sequenceNumberString);
                String simulatedGetResponse = simulateOpenSearchResponse(
                    request.index(),
                    request.id(),
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
                return GetDataObjectResponse.builder().id(request.id()).parser(parser).source(sourceAsMap).build();
            } catch (IOException e) {
                // Rethrow unchecked exception on XContent parsing error
                throw new OpenSearchStatusException("Failed to parse response", RestStatus.INTERNAL_SERVER_ERROR);
            }
        });
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
        return isGlobalResource(request.index(), request.id(), executor, isMultiTenancyEnabled).thenCompose(isGlobalResource -> {
            if (isGlobalResource) {
                return updateItem(request, globalTenantId);
            } else {
                return updateItem(request, request.tenantId() != null ? request.tenantId() : DEFAULT_TENANT);
            }
        }).exceptionally(t -> {
            log.error("Failed to check the resource type, aborting the update", t);
            Throwable cause = t.getCause() != null ? t.getCause() : t;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new CompletionException("Failed to get the item.", cause);
            }
        });
    }

    private CompletionStage<UpdateDataObjectResponse> updateItem(UpdateDataObjectRequest request, String tenantId) {
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
                        return UpdateDataObjectResponse.builder().id(request.id()).parser(createParser(simulatedUpdateResponse)).build();
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
                return DeleteDataObjectResponse.builder().id(request.id()).parser(createParser(simulatedDeleteResponse)).build();
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
                    createParser(builder.toString())
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

    private static AwsCredentialsProvider createCredentialsByAssumeRole(String assumeRoleArn) {
        AwsCredentialsProvider baseProvider = createCredentialsProvider();
        if (isNull(assumeRoleArn)) {
            return baseProvider;
        }
        StsClient stsClient = StsClient.builder().credentialsProvider(baseProvider).region(Region.AWS_GLOBAL).build();

        AwsCredentialsProvider assumeRoleProvider = StsAssumeRoleCredentialsProvider.builder()
            .stsClient(stsClient)
            .refreshRequest(req -> req.roleArn(assumeRoleArn).roleSessionName("kms-assume-session"))
            .build();
        return AwsCredentialsProviderChain.builder()
            .addCredentialsProvider(assumeRoleProvider)
            .addCredentialsProvider(baseProvider)
            .build();
    }

    @Override
    public void close() throws Exception {
        if (dynamoDbAsyncClient != null) {
            dynamoDbAsyncClient.close();
        }
        if (aosOpenSearchClient != null) {
            aosOpenSearchClient.close();
        }
    }

    /**
     * A supplier to provide KMS client by credential and region
     */
    static final class FixedCredsKmsClientSupplier implements IClientSupplier {
        private final AwsCredentialsProvider creds;
        private final Region region;

        FixedCredsKmsClientSupplier(AwsCredentialsProvider creds, Region region) {
            this.creds = creds;
            this.region = region;
        }

        @Override
        public KmsClient GetClient(GetClientInput input) {
            return KmsClient.builder().region(region).credentialsProvider(creds).build();
        }
    }

    /**
     * Create the item encryptor by table name and kms role.
     * @param tableName the table name
     * @param kmsKeyArn the kms arn role
     * @param assumeRoleArn A role to assume for cmk
     * @return encryptor to encrypt and decrypt
     */
    public DynamoDbItemEncryptor getEncryptorForTable(String tableName, String kmsKeyArn, String assumeRoleArn) {
        String[] arnParts = kmsKeyArn.split(":", 6);
        IClientSupplier supplier = new FixedCredsKmsClientSupplier(createCredentialsByAssumeRole(assumeRoleArn), Region.of(arnParts[3]));

        MaterialProviders matProv = MaterialProviders.builder().MaterialProvidersConfig(MaterialProvidersConfig.builder().build()).build();

        IKeyring kmsKeyring = matProv.CreateAwsKmsMrkMultiKeyring(
            CreateAwsKmsMrkMultiKeyringInput.builder().generator(kmsKeyArn).clientSupplier(supplier).build()
        );

        Map<String, CryptoAction> actions = new HashMap<>();
        actions.put(HASH_KEY, CryptoAction.SIGN_ONLY);
        actions.put(RANGE_KEY, CryptoAction.SIGN_ONLY);
        actions.put(SOURCE, CryptoAction.ENCRYPT_AND_SIGN);
        actions.put(SEQ_NO_KEY, CryptoAction.SIGN_ONLY);

        DynamoDbItemEncryptorConfig itemConfig = DynamoDbItemEncryptorConfig.builder()
            .logicalTableName(tableName)
            .partitionKeyName(HASH_KEY)
            .sortKeyName(RANGE_KEY)
            .attributeActionsOnEncrypt(actions)
            .allowedUnsignedAttributePrefix(":")
            .keyring(kmsKeyring)
            .build();
        return DynamoDbItemEncryptor.builder().DynamoDbItemEncryptorConfig(itemConfig).build();
    }

}
