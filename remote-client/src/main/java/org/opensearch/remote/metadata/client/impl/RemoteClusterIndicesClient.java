/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.URIScheme;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.OpType;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.MatchAllQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
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

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonParser;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_OPENSEARCH;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;

/**
 * An implementation of {@link SdkClient} that stores data in a remote
 * OpenSearch cluster using the OpenSearch Java Client.
 */
public class RemoteClusterIndicesClient extends AbstractSdkClient {
    private static final Logger log = LogManager.getLogger(RemoteClusterIndicesClient.class);

    @SuppressWarnings("unchecked")
    protected static final Class<Map<String, Object>> MAP_DOCTYPE = (Class<Map<String, Object>>) (Class<?>) Map.class;

    protected OpenSearchAsyncClient openSearchAsyncClient;
    protected JsonpMapper mapper;

    @Override
    public boolean supportsMetadataType(String metadataType) {
        return REMOTE_OPENSEARCH.equals(metadataType);
    }

    @Override
    public void initialize(Map<String, String> metadataSettings) {
        super.initialize(metadataSettings);
        this.openSearchAsyncClient = createOpenSearchAsyncClient();
        this.mapper = openSearchAsyncClient._transport().jsonpMapper();
    }

    /**
     * Empty constructor for SPI
     */
    public RemoteClusterIndicesClient() {}

    /**
     * Package Private constructor for testing
     * @param openSearchAsyncClient an OpenSearch async client (or mock for testing)
     * @param tenantIdField the tenant ID field
     */
    RemoteClusterIndicesClient(OpenSearchAsyncClient openSearchAsyncClient, String tenantIdField) {
        super.initialize(Collections.singletonMap(TENANT_ID_FIELD_KEY, tenantIdField));
        this.openSearchAsyncClient = openSearchAsyncClient;
        this.mapper = openSearchAsyncClient._transport().jsonpMapper();
    }

    @Override
    public CompletionStage<PutDataObjectResponse> putDataObjectAsync(
        PutDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        return doPrivileged(() -> {
            try {
                IndexRequest.Builder<?> builder = new IndexRequest.Builder<>().index(request.index())
                    .opType(request.overwriteIfExists() ? OpType.Index : OpType.Create)
                    .document(request.dataObject())
                    .tDocumentSerializer(new JsonTransformer.XContentObjectJsonpSerializer());
                if (shouldUseId(request.id())) {
                    builder.id(request.id());
                }
                if (request.ifSeqNo() != null) {
                    builder.ifSeqNo(request.ifSeqNo());
                }
                if (request.ifPrimaryTerm() != null) {
                    builder.ifPrimaryTerm(request.ifPrimaryTerm());
                }

                IndexRequest<?> indexRequest = builder.build();
                log.info("Indexing data object in {}", request.index());

                return openSearchAsyncClient.index(indexRequest).thenApply(indexResponse -> {
                    log.info("Creation status for id {}: {}", indexResponse.id(), indexResponse.result());
                    try {
                        return PutDataObjectResponse.builder().id(indexResponse.id()).parser(createParser(indexResponse)).build();
                    } catch (IOException e) {
                        throw new OpenSearchStatusException(
                            "Failed to create response for index " + request.index(),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                }).exceptionally(e -> {
                    Throwable cause = e.getCause();
                    if (cause instanceof OpenSearchException) {
                        OpenSearchException ose = (OpenSearchException) cause;
                        String errorType = ose.status() == RestStatus.CONFLICT.getStatus() ? "Document Version Conflict" : "Failed";
                        log.error("{} putting {} in {}: {}", errorType, request.id(), request.index(), ose.getMessage(), ose);
                        throw new OpenSearchStatusException(
                            errorType + " putting " + request.id() + " in index " + request.index(),
                            RestStatus.fromCode(ose.status()),
                            ose
                        );
                    }
                    throw new OpenSearchStatusException(
                        "Failed to put data object in index " + request.index(),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        e
                    );
                });
            } catch (IOException e) {
                log.error("Error putting data object in {}: {}", request.index(), e.getMessage(), e);
                throw new OpenSearchStatusException(
                    "Failed to parse data object to put in index " + request.index(),
                    RestStatus.BAD_REQUEST
                );
            }
        });
    }

    @Override
    public CompletionStage<GetDataObjectResponse> getDataObjectAsync(
        GetDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        if (Boolean.FALSE.equals(isMultiTenancyEnabled) || globalTenantId == null) {
            return innerGetDataObjectAsync(request);
        }
        // First check cache for global resource
        GetDataObjectResponse cachedResponse = getGlobalResourceDataFromCache(request);
        if (cachedResponse != null) {
            return CompletableFuture.completedFuture(cachedResponse);
        }

        CompletionStage<GetDataObjectResponse> dataFetched = innerGetDataObjectAsync(request);
        return handleOSDocumentBasedResponse(request, dataFetched);
    }

    @Override
    public CompletionStage<Boolean> isGlobalResource(String index, String id, Executor executor, Boolean isMultiTenancyEnabled) {
        if (Boolean.FALSE.equals(isMultiTenancyEnabled) || globalTenantId == null) {
            return CompletableFuture.completedFuture(false);
        }
        GetDataObjectRequest request = GetDataObjectRequest.builder().tenantId(globalTenantId).index(index).id(id).build();
        CompletionStage<GetDataObjectResponse> dataFetchedWithGlobalTenantId = innerGetDataObjectAsync(request);
        return dataFetchedWithGlobalTenantId.thenCompose(response -> {
            boolean isGlobalResource = isGlobalResource(response);
            if (isGlobalResource) {
                addToGlobalResourceCache(request, dataFetchedWithGlobalTenantId);
            }
            return CompletableFuture.completedFuture(isGlobalResource);
        });
    }

    private CompletionStage<GetDataObjectResponse> innerGetDataObjectAsync(GetDataObjectRequest request) {
        return doPrivileged(() -> {
            try {
                GetRequest getRequest = new GetRequest.Builder().index(request.index()).id(request.id()).build();
                log.info("Getting {} from {}", request.id(), request.index());
                return openSearchAsyncClient.get(getRequest, MAP_DOCTYPE).thenApply(getResponse -> {
                    log.info("Get found status for id {}: {}", getResponse.id(), getResponse.found());
                    Map<String, Object> source = getResponse.source();
                    try {
                        return GetDataObjectResponse.builder()
                            .id(getResponse.id())
                            .parser(createParser(getResponse))
                            .source(source)
                            .build();
                    } catch (IOException e) {
                        throw new OpenSearchStatusException(
                            "Failed to create parser for data object retrieved from index " + request.index(),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                }).exceptionally(e -> {
                    log.error("Error getting data object {} from {}: {}", request.id(), request.index(), e.getMessage(), e);
                    throw new OpenSearchStatusException(
                        "Failed to get data object from index " + request.index(),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        e
                    );
                });
            } catch (IOException e) {
                log.error("Error getting data object {} from {}: {}", request.id(), request.index(), e.getMessage(), e);
                // Rethrow unchecked exception on XContent parser creation error
                throw new OpenSearchStatusException(
                    "Failed to get data object from index " + request.index(),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e
                );
            }
        });
    }

    @Override
    public CompletionStage<UpdateDataObjectResponse> updateDataObjectAsync(
        UpdateDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        return doPrivileged(() -> {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                request.dataObject().toXContent(builder, ToXContent.EMPTY_PARAMS);
                Map<String, Object> docMap = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    builder.toString()
                ).map();
                UpdateRequest.Builder<Map<String, Object>, Map<String, Object>> updateRequestBuilder = new UpdateRequest.Builder<
                    Map<String, Object>,
                    Map<String, Object>>().index(request.index()).id(request.id()).doc(docMap);
                if (request.ifSeqNo() != null) {
                    updateRequestBuilder.ifSeqNo(request.ifSeqNo());
                }
                if (request.ifPrimaryTerm() != null) {
                    updateRequestBuilder.ifPrimaryTerm(request.ifPrimaryTerm());
                }
                if (request.retryOnConflict() > 0) {
                    updateRequestBuilder.retryOnConflict(request.retryOnConflict());
                }
                UpdateRequest<Map<String, Object>, ?> updateRequest = updateRequestBuilder.build();
                log.info("Updating {} in {}", request.id(), request.index());
                return openSearchAsyncClient.update(updateRequest, MAP_DOCTYPE).thenApply(updateResponse -> {
                    log.info("Update status for id {}: {}", updateResponse.id(), updateResponse.result());
                    try {
                        return UpdateDataObjectResponse.builder().id(updateResponse.id()).parser(createParser(updateResponse)).build();
                    } catch (IOException e) {
                        throw new OpenSearchStatusException(
                            "Failed to create parser for update response for " + request.id() + " in index " + request.index(),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                }).exceptionally(e -> {
                    Throwable cause = e.getCause();
                    if (cause instanceof OpenSearchException) {
                        OpenSearchException ose = (OpenSearchException) cause;
                        String errorType = ose.status() == RestStatus.CONFLICT.getStatus() ? "Document Version Conflict" : "Failed";
                        log.error("{} updating {} in {}: {}", errorType, request.id(), request.index(), ose.getMessage(), ose);
                        throw new OpenSearchStatusException(
                            errorType + " updating " + request.id() + " in index " + request.index(),
                            RestStatus.fromCode(ose.status()),
                            ose
                        );
                    } else {
                        log.error("Error updating {} in {}: {}", request.id(), request.index(), e.getMessage(), e);
                        throw new OpenSearchStatusException(
                            "Error updating data object " + request.id() + " in index " + request.index(),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                });
            } catch (IOException e) {
                log.error("Error preparing update for {} in {}: {}", request.id(), request.index(), e.getMessage(), e);
                throw new OpenSearchStatusException(
                    "Parsing error preparing update for data object " + request.id() + " in index " + request.index(),
                    RestStatus.BAD_REQUEST,
                    e
                );
            }
        });
    }

    @Override
    public CompletionStage<DeleteDataObjectResponse> deleteDataObjectAsync(
        DeleteDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        return doPrivileged(() -> {
            try {
                DeleteRequest.Builder builder = new DeleteRequest.Builder().index(request.index()).id(request.id());

                if (request.ifSeqNo() != null) {
                    builder.ifSeqNo(request.ifSeqNo());
                }
                if (request.ifPrimaryTerm() != null) {
                    builder.ifPrimaryTerm(request.ifPrimaryTerm());
                }

                DeleteRequest deleteRequest = builder.build();
                log.info("Deleting {} from {}", request.id(), request.index());

                return openSearchAsyncClient.delete(deleteRequest).thenApply(deleteResponse -> {
                    log.info("Deletion status for id {}: {}", deleteResponse.id(), deleteResponse.result());
                    try {
                        return DeleteDataObjectResponse.builder().id(deleteResponse.id()).parser(createParser(deleteResponse)).build();
                    } catch (IOException e) {
                        throw new OpenSearchStatusException(
                            "Failed to create parser for delete response for " + request.id() + " from index " + request.index(),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                }).exceptionally(e -> {
                    Throwable cause = e.getCause();
                    if (cause instanceof OpenSearchException) {
                        OpenSearchException ose = (OpenSearchException) cause;
                        String errorType = ose.status() == RestStatus.CONFLICT.getStatus() ? "Document Version Conflict" : "Failed";
                        log.error("{} deleting {} from {}: {}", errorType, request.id(), request.index(), ose.getMessage(), ose);
                        throw new OpenSearchStatusException(
                            errorType + " deleting " + request.id() + " from index " + request.index(),
                            RestStatus.fromCode(ose.status()),
                            ose
                        );
                    }
                    throw new OpenSearchStatusException(
                        "Failed to delete data object " + request.id() + " from index " + request.index(),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        e
                    );
                });
            } catch (IOException e) {
                log.error("Error initiating delete for {} from {}: {}", request.id(), request.index(), e.getMessage(), e);
                throw new OpenSearchStatusException(
                    "Failed to delete data object " + request.id() + " from index " + request.index(),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e
                );
            }
        });
    }

    @Override
    public CompletionStage<BulkDataObjectResponse> bulkDataObjectAsync(
        BulkDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        return doPrivileged(() -> {
            try {
                log.info("Performing {} bulk actions on indices {}", request.requests().size(), request.getIndices());
                List<BulkOperation> operations = new ArrayList<>();
                for (DataObjectRequest dataObjectRequest : request.requests()) {
                    addBulkOperation(dataObjectRequest, operations);
                }
                BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).refresh(Refresh.True).build();
                return openSearchAsyncClient.bulk(bulkRequest).thenApply(bulkResponse -> {
                    log.info(
                        "Bulk action complete for {} items: {}",
                        bulkResponse.items().size(),
                        bulkResponse.errors() ? "has failures" : "success"
                    );
                    try {
                        DataObjectResponse[] responses = bulkResponseItemsToArray(bulkResponse.items());
                        return bulkResponse.ingestTook() == null
                            ? new BulkDataObjectResponse(responses, bulkResponse.took(), bulkResponse.errors(), createParser(bulkResponse))
                            : new BulkDataObjectResponse(
                                responses,
                                bulkResponse.took(),
                                bulkResponse.ingestTook().longValue(),
                                bulkResponse.errors(),
                                createParser(bulkResponse)
                            );
                    } catch (IOException e) {
                        throw new OpenSearchStatusException(
                            "Failed to parse data object in a bulk response",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                })
                    .exceptionally(
                        e -> { throw new OpenSearchStatusException("Failed to execute bulk request", RestStatus.INTERNAL_SERVER_ERROR, e); }
                    );
            } catch (Exception e) {
                throw new OpenSearchStatusException("Failed to execute bulk request", RestStatus.INTERNAL_SERVER_ERROR, e);
            }
        });
    }

    private void addBulkOperation(DataObjectRequest dataObjectRequest, List<BulkOperation> operations) {
        if (dataObjectRequest instanceof PutDataObjectRequest) {
            addBulkPutOperation((PutDataObjectRequest) dataObjectRequest, operations);
        } else if (dataObjectRequest instanceof UpdateDataObjectRequest) {
            addBulkUpdateOperation((UpdateDataObjectRequest) dataObjectRequest, operations);
        } else if (dataObjectRequest instanceof DeleteDataObjectRequest) {
            addBulkDeleteOperation((DeleteDataObjectRequest) dataObjectRequest, operations);
        } else {
            throw new IllegalArgumentException("Invalid type for bulk request");
        }
    }

    private void addBulkPutOperation(PutDataObjectRequest putRequest, List<BulkOperation> operations) {
        if (putRequest.overwriteIfExists()) {
            // Use index operation
            operations.add(BulkOperation.of(op -> op.index(i -> {
                i.index(putRequest.index())
                    .document(putRequest.dataObject())
                    .tDocumentSerializer(new JsonTransformer.XContentObjectJsonpSerializer());
                if (!Strings.isNullOrEmpty(putRequest.id())) {
                    i.id(putRequest.id());
                }
                if (putRequest.ifSeqNo() != null) {
                    i.ifSeqNo(putRequest.ifSeqNo());
                }
                if (putRequest.ifPrimaryTerm() != null) {
                    i.ifPrimaryTerm(putRequest.ifPrimaryTerm());
                }
                return i;
            })));
        } else {
            // Use create operation (no seqNo or primary term)
            operations.add(BulkOperation.of(op -> op.create(c -> {
                c.index(putRequest.index())
                    .document(putRequest.dataObject())
                    .tDocumentSerializer(new JsonTransformer.XContentObjectJsonpSerializer());
                if (!Strings.isNullOrEmpty(putRequest.id())) {
                    c.id(putRequest.id());
                }
                return c;
            })));
        }
    }

    private void addBulkUpdateOperation(UpdateDataObjectRequest updateRequest, List<BulkOperation> operations) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            updateRequest.dataObject().toXContent(builder, ToXContent.EMPTY_PARAMS);
            Map<String, Object> docMap = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                builder.toString()
            ).map();
            operations.add(BulkOperation.of(op -> op.update(u -> {
                u.index(updateRequest.index()).id(updateRequest.id()).document(docMap);
                if (updateRequest.ifSeqNo() != null) {
                    u.ifSeqNo(updateRequest.ifSeqNo());
                }
                if (updateRequest.ifPrimaryTerm() != null) {
                    u.ifPrimaryTerm(updateRequest.ifPrimaryTerm());
                }
                if (updateRequest.retryOnConflict() > 0) {
                    u.retryOnConflict(updateRequest.retryOnConflict());
                }
                return u;
            })));
        } catch (IOException e) {
            // Rethrow unchecked exception on XContent parsing error
            throw new OpenSearchStatusException("Failed to parse data object in a bulk update request", RestStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void addBulkDeleteOperation(DeleteDataObjectRequest deleteRequest, List<BulkOperation> operations) {
        operations.add(BulkOperation.of(op -> op.delete(d -> {
            d.index(deleteRequest.index()).id(deleteRequest.id());
            if (deleteRequest.ifSeqNo() != null) {
                d.ifSeqNo(deleteRequest.ifSeqNo());
            }
            if (deleteRequest.ifPrimaryTerm() != null) {
                d.ifPrimaryTerm(deleteRequest.ifPrimaryTerm());
            }
            return d;
        })));
    }

    private DataObjectResponse[] bulkResponseItemsToArray(List<BulkResponseItem> items) throws IOException {
        DataObjectResponse[] responses = new DataObjectResponse[items.size()];
        int i = 0;
        for (BulkResponseItem itemResponse : items) {
            switch (itemResponse.operationType()) {
                case Index:
                case Create:
                    responses[i++] = PutDataObjectResponse.builder()
                        .id(itemResponse.id())
                        .parser(createParser(itemResponse))
                        .failed(itemResponse.error() != null)
                        .build();
                    break;
                case Update:
                    responses[i++] = UpdateDataObjectResponse.builder()
                        .id(itemResponse.id())
                        .parser(createParser(itemResponse))
                        .failed(itemResponse.error() != null)
                        .build();
                    break;
                case Delete:
                    responses[i++] = DeleteDataObjectResponse.builder()
                        .id(itemResponse.id())
                        .parser(createParser(itemResponse))
                        .failed(itemResponse.error() != null)
                        .build();
                    break;
                default:
                    throw new OpenSearchStatusException("Invalid operation type for bulk response", RestStatus.INTERNAL_SERVER_ERROR);
            }
        }
        return responses;
    }

    @Override
    public CompletionStage<SearchDataObjectResponse> searchDataObjectAsync(
        SearchDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        return doPrivileged(() -> {
            try {
                log.info("Searching {}", Arrays.toString(request.indices()));
                JsonParser parser = mapper.jsonProvider().createParser(new StringReader(request.searchSourceBuilder().toString()));
                SearchRequest searchRequest = SearchRequest._DESERIALIZER.deserialize(parser, mapper);
                if (Boolean.TRUE.equals(isMultiTenancyEnabled)) {
                    if (request.tenantId() == null) {
                        throw new OpenSearchStatusException("Tenant ID is required when multitenancy is enabled.", RestStatus.BAD_REQUEST);
                    }
                    TermQuery tenantIdFilterQuery = new TermQuery.Builder().field(this.tenantIdField)
                        .value(FieldValue.of(request.tenantId()))
                        .build();
                    Query existingQuery = searchRequest.query();
                    BoolQuery boolQuery = new BoolQuery.Builder().must(
                        existingQuery == null ? new MatchAllQuery.Builder().build().toQuery() : existingQuery
                    ).filter(tenantIdFilterQuery.toQuery()).build();
                    searchRequest = searchRequest.toBuilder().index(Arrays.asList(request.indices())).query(boolQuery.toQuery()).build();
                } else {
                    searchRequest = searchRequest.toBuilder().index(Arrays.asList(request.indices())).build();
                }

                return openSearchAsyncClient.search(searchRequest, MAP_DOCTYPE).thenApply(searchResponse -> {
                    log.info("Search returned {} hits", searchResponse.hits().total().value());
                    try {
                        return SearchDataObjectResponse.builder().parser(createParser(searchResponse)).build();
                    } catch (IOException e) {
                        throw new OpenSearchStatusException(
                            "Failed to create parser for search response",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        );
                    }
                }).exceptionally(e -> {
                    Throwable cause = e;
                    while (cause instanceof CompletionException && cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    log.error("Error searching {}: {}", Arrays.toString(request.indices()), cause.getMessage(), cause);
                    if (cause instanceof OpenSearchException) {
                        throw new OpenSearchStatusException(
                            "Failed to search indices " + Arrays.toString(request.indices()),
                            RestStatus.fromCode(((OpenSearchException) cause).status()),
                            cause
                        );
                    }
                    throw new OpenSearchStatusException(
                        "Failed to search indices " + Arrays.toString(request.indices()),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        e
                    );
                });
            } catch (IOException e) {
                log.error("Error preparing search for {}: {}", Arrays.toString(request.indices()), e.getMessage(), e);
                throw new OpenSearchStatusException(
                    "Failed to prepare search for indices " + Arrays.toString(request.indices()),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e
                );
            }
        });
    }

    private XContentParser createParser(JsonpSerializable obj) throws IOException {
        StringWriter stringWriter = new StringWriter();
        try (JsonGenerator generator = mapper.jsonProvider().createGenerator(stringWriter)) {
            mapper.serialize(obj, generator);
        }
        return SdkClientUtils.createParser(stringWriter.toString());
    }

    /**
     * Create an instance of {@link OpenSearchAsyncClient}
     * @return An OpenSearchAsyncClient instance
     */
    protected OpenSearchAsyncClient createOpenSearchAsyncClient() {
        try {
            Map<String, String> env = System.getenv();
            String user = env.getOrDefault("user", "admin");
            String pass = env.getOrDefault("password", "admin");
            // Endpoint syntax: https://127.0.0.1:9200
            HttpHost host = HttpHost.create(remoteMetadataEndpoint);
            SSLContext sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (chain, authType) -> true).build();

            ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder.builder(host)
                .setMapper(
                    new JacksonJsonpMapper(
                        new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                            .registerModule(new JavaTimeModule())
                            .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                    )
                )
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials(user, pass.toCharArray()));
                    if (URIScheme.HTTP.getId().equalsIgnoreCase(host.getSchemeName())) {
                        // No SSL/TLS
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                    // Disable SSL/TLS verification as our local testing clusters use self-signed certificates
                    final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                        .setSslContext(sslContext)
                        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .buildAsync();
                    final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                        .setTlsStrategy(tlsStrategy)
                        .build();
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
                })
                .build();

            return new OpenSearchAsyncClient(transport);
        } catch (Exception e) {
            throw new org.opensearch.OpenSearchException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (openSearchAsyncClient != null && openSearchAsyncClient._transport() != null) {
            openSearchAsyncClient._transport().close();
        }
    }
}
