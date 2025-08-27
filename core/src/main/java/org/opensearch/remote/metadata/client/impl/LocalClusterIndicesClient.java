/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.remote.metadata.client.AbstractSdkClient;
import org.opensearch.remote.metadata.client.BulkDataObjectRequest;
import org.opensearch.remote.metadata.client.BulkDataObjectResponse;
import org.opensearch.remote.metadata.client.DataObjectRequest;
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
import org.opensearch.remote.metadata.client.WriteDataObjectRequest;
import org.opensearch.remote.metadata.common.CommonValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * An implementation of {@link SdkClient} that stores data in a local OpenSearch
 * cluster using the Node Client.
 */
public class LocalClusterIndicesClient extends AbstractSdkClient {
    private static final Logger log = LogManager.getLogger(LocalClusterIndicesClient.class);
    private static final long DEFAULT_REFRESH_MINUTES = 5;
    private static final Map<String, Map<String, Object>> GLOBAL_RESOURCES_CACHE = new ConcurrentHashMap<>();

    private final Client client;
    private final String GLOBAL_TENANT_ID;
    private final TimeValue CACHE_REFRESH_INTERVAL;
    private Scheduler.Cancellable globalResourcesCacheScheduler;

    @Override
    public boolean supportsMetadataType(String metadataType) {
        return Strings.isNullOrEmpty(metadataType);
    }

    /**
     * Instantiate this client
     * @param client The OpenSearch Node Client. May be null if the implementation doesn't need it.
     * @param xContentRegistry The OpenSearch NamedXContentRegistry. May be null if the implementation doesn't need it.
     * @param metadataSettings The map of metadata settings.
     */
    public LocalClusterIndicesClient(Client client, NamedXContentRegistry xContentRegistry, Map<String, String> metadataSettings) {
        super.initialize(metadataSettings);
        globalResourcesCacheScheduler = null;
        GLOBAL_TENANT_ID = metadataSettings.get(CommonValue.REMOTE_METADATA_GLOBAL_TENANT_ID_KEY);
        CACHE_REFRESH_INTERVAL = Optional.ofNullable(metadataSettings.get(CommonValue.REMOTE_METADATA_CACHE_REFRESH_INTERVAL_KEY))
            .map(value -> {
                try {
                    return TimeValue.timeValueMinutes(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    return TimeValue.timeValueMinutes(DEFAULT_REFRESH_MINUTES);
                }
            })
            .orElse(TimeValue.timeValueMinutes(DEFAULT_REFRESH_MINUTES));
        if (GLOBAL_TENANT_ID != null) {
            cacheGlobalResources();
            startGlobalResourcesCacheScheduler();
        }
        this.client = client;
    }

    @Override
    public CompletionStage<PutDataObjectResponse> putDataObjectAsync(
        PutDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        CompletableFuture<PutDataObjectResponse> future = new CompletableFuture<>();
        return doPrivileged(() -> {
            try {
                log.info("Indexing data object in {}", request.index());
                IndexRequest indexRequest = createIndexRequest(request).setRefreshPolicy(IMMEDIATE);
                client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                    log.info("Creation status for id {}: {}", indexResponse.getId(), indexResponse.getResult());
                    future.complete(new PutDataObjectResponse(indexResponse));
                }, e -> {
                    Throwable t = ExceptionsHelper.unwrapCause(e);
                    if (t instanceof VersionConflictEngineException) {
                        log.error("Document version conflict putting {} in {}: {}", request.id(), request.index(), e.getMessage(), e);
                        future.completeExceptionally(
                            new OpenSearchStatusException(
                                "Document version conflict putting " + request.id() + " in index " + request.index(),
                                RestStatus.CONFLICT,
                                t
                            )
                        );
                    } else {
                        future.completeExceptionally(
                            new OpenSearchStatusException(
                                "Failed to put data object in index " + request.index(),
                                RestStatus.INTERNAL_SERVER_ERROR,
                                e
                            )
                        );
                    }
                }));
            } catch (IOException e) {
                future.completeExceptionally(
                    new OpenSearchStatusException(
                        "Failed to parse data object to put in index " + request.index(),
                        RestStatus.BAD_REQUEST,
                        e
                    )
                );
            }
            return future;
        });
    }

    private IndexRequest createIndexRequest(PutDataObjectRequest putDataObjectRequest) throws IOException {
        try (XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(putDataObjectRequest.index()).opType(
                putDataObjectRequest.overwriteIfExists() ? OpType.INDEX : OpType.CREATE
            ).source(putDataObjectRequest.dataObject().toXContent(sourceBuilder, EMPTY_PARAMS));
            if (shouldUseId(putDataObjectRequest.id())) {
                indexRequest.id(putDataObjectRequest.id());
            }
            return setSeqNoAndPrimaryTerm(indexRequest, putDataObjectRequest);
        }
    }

    private <T extends DocWriteRequest<T>> T setSeqNoAndPrimaryTerm(T request, WriteDataObjectRequest writeRequest) {
        if (writeRequest.ifSeqNo() != null) {
            request.setIfSeqNo(writeRequest.ifSeqNo());
        }
        if (writeRequest.ifPrimaryTerm() != null) {
            request.setIfPrimaryTerm(writeRequest.ifPrimaryTerm());
        }
        return request;
    }

    @Override
    public CompletionStage<GetDataObjectResponse> getDataObjectAsync(
        GetDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        CompletableFuture<GetDataObjectResponse> future = new CompletableFuture<>();
        return doPrivileged(() -> {
            GetRequest getRequest = createGetRequest(request);
            client.get(
                getRequest,
                ActionListener.wrap(
                    // Only fallback to global resource when GLOBAL_TENANT_ID is set && not found in index
                    getResponse -> completeWithGlobalResourceIfNotFound(getResponse, request, future),
                    e -> future.completeExceptionally(
                        new OpenSearchStatusException(
                            "Failed to get data object from index " + request.index(),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        )
                    )
                )
            );
            return future;
        });
    }

    private void completeWithGlobalResourceIfNotFound(
        org.opensearch.action.get.GetResponse response,
        GetDataObjectRequest request,
        CompletableFuture<GetDataObjectResponse> future
    ) {
        if (!response.isExists() && GLOBAL_TENANT_ID != null && !GLOBAL_TENANT_ID.equals(request.tenantId())) {
            CompletionStage<GetDataObjectResponse> cachedResponse = getGlobalResourceDataFromCache(request);
            if (cachedResponse != null) {
                cachedResponse.thenAccept(future::complete);
                return;
            }
        }
        future.complete(new GetDataObjectResponse(response));
    }

    private GetRequest createGetRequest(GetDataObjectRequest request) {
        return new GetRequest(request.index(), request.id()).fetchSourceContext(request.fetchSourceContext());
    }

    @Override
    public CompletionStage<UpdateDataObjectResponse> updateDataObjectAsync(
        UpdateDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        CompletableFuture<UpdateDataObjectResponse> future = new CompletableFuture<>();
        return doPrivileged(() -> {
            try {
                log.info("Updating {} from {}", request.id(), request.index());
                UpdateRequest updateRequest = createUpdateRequest(request);
                client.update(updateRequest, ActionListener.wrap(updateResponse -> {
                    if (updateResponse == null) {
                        log.info("Null UpdateResponse");
                        future.complete(UpdateDataObjectResponse.builder().id(request.id()).parser(null).build());
                    } else {
                        log.info("Update status for id {}: {}", updateResponse.getId(), updateResponse.getResult());
                        future.complete(new UpdateDataObjectResponse(updateResponse));
                    }
                }, e -> {
                    Throwable t = ExceptionsHelper.unwrapCause(e);
                    if (t instanceof VersionConflictEngineException) {
                        log.error("Document version conflict updating {} in {}: {}", request.id(), request.index(), e.getMessage(), e);
                        future.completeExceptionally(
                            new OpenSearchStatusException(
                                "Document version conflict updating " + request.id() + " in index " + request.index(),
                                RestStatus.CONFLICT,
                                t
                            )
                        );
                    } else {
                        future.completeExceptionally(
                            new OpenSearchStatusException(
                                "Failed to update data object in index " + request.index(),
                                RestStatus.INTERNAL_SERVER_ERROR,
                                t
                            )
                        );
                    }
                }));
            } catch (IOException e) {
                future.completeExceptionally(
                    new OpenSearchStatusException(
                        "Failed to parse data object to update in index " + request.index(),
                        RestStatus.BAD_REQUEST,
                        e
                    )
                );
            }
            return future;
        });
    }

    private UpdateRequest createUpdateRequest(UpdateDataObjectRequest updateDataObjectRequest) throws IOException {
        try (XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()) {
            UpdateRequest updateRequest = new UpdateRequest(updateDataObjectRequest.index(), updateDataObjectRequest.id()).doc(
                updateDataObjectRequest.dataObject().toXContent(sourceBuilder, EMPTY_PARAMS)
            );
            if (updateDataObjectRequest.retryOnConflict() > 0) {
                updateRequest.retryOnConflict(updateDataObjectRequest.retryOnConflict());
            }
            return setSeqNoAndPrimaryTerm(updateRequest, updateDataObjectRequest);
        }
    }

    @Override
    public CompletionStage<DeleteDataObjectResponse> deleteDataObjectAsync(
        DeleteDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        CompletableFuture<DeleteDataObjectResponse> future = new CompletableFuture<>();
        return doPrivileged(() -> {
            log.info("Deleting {} from {}", request.id(), request.index());
            DeleteRequest deleteRequest = createDeleteRequest(request).setRefreshPolicy(IMMEDIATE);
            client.delete(deleteRequest, ActionListener.wrap(deleteResponse -> {
                log.info("Deletion status for id {}: {}", deleteResponse.getId(), deleteResponse.getResult());
                future.complete(new DeleteDataObjectResponse(deleteResponse));
            }, e -> {
                Throwable t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof VersionConflictEngineException) {
                    log.error("Document version conflict deleting {} from {}: {}", request.id(), request.index(), e.getMessage(), e);
                    future.completeExceptionally(
                        new OpenSearchStatusException(
                            "Document version conflict deleting " + request.id() + " from index " + request.index(),
                            RestStatus.CONFLICT,
                            t
                        )
                    );
                } else {
                    future.completeExceptionally(
                        new OpenSearchStatusException(
                            "Failed to delete data object from index " + request.index(),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e
                        )
                    );
                }
            }));
            return future;
        });
    }

    private DeleteRequest createDeleteRequest(DeleteDataObjectRequest deleteDataObjectRequest) {
        DeleteRequest deleteRequest = new DeleteRequest(deleteDataObjectRequest.index(), deleteDataObjectRequest.id());
        return setSeqNoAndPrimaryTerm(deleteRequest, deleteDataObjectRequest);
    }

    @Override
    public CompletionStage<BulkDataObjectResponse> bulkDataObjectAsync(
        BulkDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        CompletableFuture<BulkDataObjectResponse> future = new CompletableFuture<>();
        return doPrivileged(() -> {
            try {
                log.info("Performing {} bulk actions on indices {}", request.requests().size(), request.getIndices());
                BulkRequest bulkRequest = new BulkRequest();

                for (DataObjectRequest dataObjectRequest : request.requests()) {
                    if (dataObjectRequest instanceof PutDataObjectRequest) {
                        bulkRequest.add(createIndexRequest((PutDataObjectRequest) dataObjectRequest));
                    } else if (dataObjectRequest instanceof UpdateDataObjectRequest) {
                        bulkRequest.add(createUpdateRequest((UpdateDataObjectRequest) dataObjectRequest));
                    } else if (dataObjectRequest instanceof DeleteDataObjectRequest) {
                        bulkRequest.add(createDeleteRequest((DeleteDataObjectRequest) dataObjectRequest));
                    }
                }
                client.bulk(bulkRequest.setRefreshPolicy(IMMEDIATE), ActionListener.wrap(bulkResponse -> {
                    future.complete(new BulkDataObjectResponse(bulkResponse));
                },
                    e -> future.completeExceptionally(
                        new OpenSearchStatusException("Failed to execute bulk request", RestStatus.INTERNAL_SERVER_ERROR, e)
                    )
                ));
            } catch (IOException e) {
                future.completeExceptionally(new OpenSearchStatusException("Failed to create bulk request", RestStatus.BAD_REQUEST, e));
            }
            return future;
        });
    }

    @Override
    public CompletionStage<SearchDataObjectResponse> searchDataObjectAsync(
        SearchDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        CompletableFuture<SearchDataObjectResponse> future = new CompletableFuture<>();

        SearchSourceBuilder searchSource = request.searchSourceBuilder();
        if (Boolean.TRUE.equals(isMultiTenancyEnabled)) {
            if (request.tenantId() == null) {
                future.completeExceptionally(
                    new OpenSearchStatusException("Tenant ID is required when multitenancy is enabled.", RestStatus.BAD_REQUEST)
                );
                return future;
            }
            QueryBuilder existingQuery = searchSource.query();
            TermQueryBuilder tenantIdTermQuery = QueryBuilders.termQuery(this.tenantIdField, request.tenantId());
            if (existingQuery == null) {
                searchSource.query(tenantIdTermQuery);
            } else {
                BoolQueryBuilder boolQuery = existingQuery instanceof BoolQueryBuilder
                    ? (BoolQueryBuilder) existingQuery
                    : QueryBuilders.boolQuery().must(existingQuery);
                boolQuery.filter(tenantIdTermQuery);
                searchSource.query(boolQuery);
            }
            log.debug("Adding tenant id {} to search query", Arrays.toString(request.indices()));
        }
        log.info("Searching {}", Arrays.toString(request.indices()));
        return doPrivileged(() -> {
            SearchRequest searchRequest = new SearchRequest(request.indices(), searchSource);
            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                log.info("Search returned {} hits", searchResponse.getHits().getTotalHits());
                future.complete(new SearchDataObjectResponse(searchResponse));
            },
                e -> future.completeExceptionally(
                    new OpenSearchStatusException(
                        "Failed to search indices " + Arrays.toString(request.indices()),
                        e instanceof IndexNotFoundException ? RestStatus.NOT_FOUND : RestStatus.INTERNAL_SERVER_ERROR,
                        e
                    )
                )
            ));
            return future;
        });
    }

    @Override
    public void close() throws Exception {
        // the NodeClient is managed by OpenSearch, only cache refresh scheduler needs to be canceled manually
        stopGlobalResourcesCacheScheduler();
    }

    @Override
    public boolean isGlobalResource(String index, String id) {
        return GLOBAL_RESOURCES_CACHE.containsKey(buildGlobalCacheKey(index, id));
    }

    private void cacheGlobalResources() {
        if (client == null || this.tenantIdField == null || this.GLOBAL_TENANT_ID == null) {
            log.warn("Unable to cache global resources");
            return;
        }
        try {
            // Create a term query for the global tenant ID like the following:
            // {"query": {"term": {"<tenantIdField>": {"value": "<GLOBAL_TENANT_ID>"}}}}
            TermQueryBuilder termQuery = QueryBuilders.termQuery(this.tenantIdField, this.GLOBAL_TENANT_ID);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(termQuery).fetchSource(true).size(10000);
            SearchRequest request = new SearchRequest();
            request.source(searchSourceBuilder);

            client.searchAsync(request).whenComplete((searchResponse, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to cache global resources", throwable);
                    return;
                }
                log.info("Found {} global resources", searchResponse.getHits().getTotalHits());

                // Cache the global resources
                searchResponse.getHits().forEach(hit -> {
                    String index = hit.getIndex();
                    String id = hit.getId();
                    Map<String, Object> source = hit.getSourceAsMap();
                    GLOBAL_RESOURCES_CACHE.put(buildGlobalCacheKey(index, id), source);
                });
                log.info("Global resources cache updated with {} entries", GLOBAL_RESOURCES_CACHE.size());
            });
        } catch (Exception e) {
            log.error("Error setting up global resources cache", e);
        }
    }

    private CompletionStage<GetDataObjectResponse> getGlobalResourceDataFromCache(GetDataObjectRequest request) {
        String cacheKey = buildGlobalCacheKey(request.index(), request.id());
        if (GLOBAL_RESOURCES_CACHE.containsKey(cacheKey)) {
            Map<String, Object> cachedSource = GLOBAL_RESOURCES_CACHE.get(cacheKey);
            // Replace tenant ID in cached response to match request tenant
            Map<String, Object> modifiedSource = new java.util.HashMap<>(cachedSource);
            modifiedSource.put(CommonValue.TENANT_ID_FIELD_KEY, request.tenantId());

            try {
                // Create a mock GetResponse with cached data
                String responseJson = String.format(
                    Locale.ROOT,
                    "{\"_index\":\"%s\",\"_id\":\"%s\",\"found\":true,\"_source\":%s}",
                    request.index(),
                    request.id(),
                    new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(modifiedSource)
                );

                return CompletableFuture.completedFuture(
                    GetDataObjectResponse.builder()
                        .id(request.id())
                        .parser(org.opensearch.remote.metadata.common.SdkClientUtils.createParser(responseJson))
                        .source(modifiedSource)
                        .build()
                );
            } catch (Exception e) {
                log.error("Failed to create response from cached global resource", e);
                return null;
            }
        }
        return null;
    }

    private void startGlobalResourcesCacheScheduler() {
        if (this.threadPool == null) {
            log.warn("ThreadPool is not available, global resources cache will not be automatically refreshed");
            return;
        }

        log.info("Starting global resources cache scheduler with refresh interval: {}", CACHE_REFRESH_INTERVAL);
        globalResourcesCacheScheduler = threadPool.scheduleWithFixedDelay(() -> {
            try {
                log.debug("Refreshing global resources cache");
                cacheGlobalResources();
            } catch (Exception e) {
                log.error("Error refreshing global resources cache", e);
            }
        }, CACHE_REFRESH_INTERVAL, ThreadPool.Names.GENERIC);
    }

    private void stopGlobalResourcesCacheScheduler() {
        if (this.globalResourcesCacheScheduler != null) {
            this.globalResourcesCacheScheduler.cancel();
            this.globalResourcesCacheScheduler = null;
            log.info("Stopped global resources cache scheduler");
        }
    }
}
