/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.remote.metadata.common.CommonValue.AWS_OPENSEARCH_SERVICE;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_GLOBAL_TENANT_ID_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.VALID_AWS_OPENSEARCH_SERVICE_NAMES;

/**
 * An implementation of {@link SdkClient} that stores data in a remote
 * OpenSearch cluster using the OpenSearch Java Client.
 */
public class AOSOpenSearchClient extends RemoteClusterIndicesClient {
    private static final Logger log = LogManager.getLogger(AOSOpenSearchClient.class);

    private Scheduler.Cancellable globalResourcesCacheScheduler;
    public static String globalTenantId;
    private static final Map<String, Map<String, Object>> GLOBAL_RESOURCES_CACHE = new ConcurrentHashMap<>();
    private static final TimeValue CACHE_REFRESH_INTERVAL = TimeValue.timeValueMinutes(5);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public boolean supportsMetadataType(String metadataType) {
        return AWS_OPENSEARCH_SERVICE.equals(metadataType);
    }

    @Override
    public void initialize(Map<String, String> metadataSettings) {
        super.initialize(metadataSettings);
        this.openSearchAsyncClient = createOpenSearchAsyncClient();
        this.mapper = openSearchAsyncClient._transport().jsonpMapper();
        globalTenantId = metadataSettings.get(REMOTE_METADATA_GLOBAL_TENANT_ID_KEY);
        if (globalTenantId != null) {
            cacheGlobalResources();
            startGlobalResourcesCacheScheduler();
        }
    }

    /**
     * Empty constructor for SPI
     */
    public AOSOpenSearchClient() {}

    @Override
    public CompletionStage<Boolean> isGlobalResource(String index, String id) {
        if (globalTenantId == null) {
            return CompletableFuture.completedFuture(false);
        }

        return doPrivileged(() -> {
            GetRequest getRequest = new GetRequest.Builder().index(index).id(id).build();
            try {
                return openSearchAsyncClient.get(getRequest, MAP_DOCTYPE).thenApply(getResponse -> {
                    if (getResponse.found() && getResponse.source() != null && getResponse.source().containsKey(tenantIdField)) {
                        Object tenantId = getResponse.source().get(tenantIdField);
                        return globalTenantId.equals(tenantId);
                    } else {
                        return false;
                    }
                }).exceptionally(e -> {
                    log.error("Error checking if resource is global for index: {} id: {}", index, id, e);
                    return false;
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void cacheGlobalResources() {
        // Search for all documents with global tenant ID across all indices
        TermQuery globalTenantQuery = new TermQuery.Builder().field(this.tenantIdField).value(FieldValue.of(globalTenantId)).build();

        SearchRequest searchRequest = new SearchRequest.Builder().index("*")
            .query(globalTenantQuery.toQuery())
            .size(10000) // Adjust size as needed
            .build();

        try {
            openSearchAsyncClient.search(searchRequest, MAP_DOCTYPE).thenAccept(searchResponse -> {
                log.info("Found {} global resources", searchResponse.hits().total().value());
                searchResponse.hits().hits().forEach(hit -> {
                    String cacheKey = buildGlobalCacheKey(hit.index(), hit.id());
                    GLOBAL_RESOURCES_CACHE.put(cacheKey, hit.source());
                });
            }).exceptionally(ex -> {
                log.error("Failed to cache global resources", ex);
                return null;
            });
        } catch (Exception e) {
            log.error("Error during global resources caching", e);
        }
    }

    private void startGlobalResourcesCacheScheduler() {
        if (threadPool != null) {
            log.info("Starting global resources cache scheduler with interval: {}", CACHE_REFRESH_INTERVAL);
            globalResourcesCacheScheduler = threadPool.scheduleWithFixedDelay(
                this::cacheGlobalResources,
                CACHE_REFRESH_INTERVAL,
                ThreadPool.Names.GENERIC
            );
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

    private void validateAwsParams() {
        if (Strings.isNullOrEmpty(remoteMetadataEndpoint) || Strings.isNullOrEmpty(region)) {
            throw new org.opensearch.OpenSearchException(remoteMetadataType + " client requires a metadata endpoint and region.");
        }
        if (!VALID_AWS_OPENSEARCH_SERVICE_NAMES.contains(serviceName)) {
            throw new org.opensearch.OpenSearchException(
                remoteMetadataType + " client only supports service names " + VALID_AWS_OPENSEARCH_SERVICE_NAMES
            );
        }
    }

    @Override
    protected OpenSearchAsyncClient createOpenSearchAsyncClient() {
        validateAwsParams();
        return createAwsOpenSearchServiceAsyncClient();
    }

    private OpenSearchAsyncClient createAwsOpenSearchServiceAsyncClient() {
        // https://github.com/opensearch-project/opensearch-java/blob/main/guides/auth.md
        final SdkHttpClient httpClient = ApacheHttpClient.builder().build();
        return new OpenSearchAsyncClient(
            doPrivileged(
                () -> new AwsSdk2Transport(
                    httpClient,
                    remoteMetadataEndpoint.replaceAll("^https?://", ""), // OpenSearch endpoint, without https://
                    serviceName, // signing service name, use "es" for OpenSearch, "aoss" for OpenSearch Serverless
                    Region.of(region), // signing service region
                    AwsSdk2TransportOptions.builder().setCredentials(createCredentialsProvider()).build()
                )
            )
        );
    }

    private static AwsCredentialsProvider createCredentialsProvider() {
        return AwsCredentialsProviderChain.builder()
            .addCredentialsProvider(EnvironmentVariableCredentialsProvider.create())
            .addCredentialsProvider(ContainerCredentialsProvider.builder().build())
            .addCredentialsProvider(InstanceProfileCredentialsProvider.create())
            .build();
    }

    @Override
    public CompletionStage<GetDataObjectResponse> getDataObjectAsync(
        GetDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    ) {
        if (globalTenantId == null) {
            // No global tenant support, use parent implementation
            return super.getDataObjectAsync(request, executor, isMultiTenancyEnabled);
        } else {
            // First check cache for global resource
            GetDataObjectResponse cachedResponse = getGlobalResourceDataFromCache(request);
            if (cachedResponse != null) {
                return CompletableFuture.completedFuture(cachedResponse);
            }

            // Try with user tenant ID first
            return super.getDataObjectAsync(request, executor, isMultiTenancyEnabled).thenCompose(response -> {
                // Check if we found data with user tenant
                if (response != null && response.source() != null && !response.source().isEmpty()) {
                    return CompletableFuture.completedFuture(response);
                }

                // If not found, try with global tenant ID
                GetDataObjectRequest globalRequest = GetDataObjectRequest.builder(request).tenantId(globalTenantId).build();
                return super.getDataObjectAsync(globalRequest, executor, isMultiTenancyEnabled);
            });
        }
    }

    private GetDataObjectResponse getGlobalResourceDataFromCache(GetDataObjectRequest request) {
        String cacheKey = buildGlobalCacheKey(request.index(), request.id());
        if (GLOBAL_RESOURCES_CACHE.containsKey(cacheKey)) {
            Map<String, Object> cachedSource = GLOBAL_RESOURCES_CACHE.get(cacheKey);
            // Replace tenant ID in cached response to match request tenant
            Map<String, Object> modifiedSource = new HashMap<>(cachedSource);
            modifiedSource.put(TENANT_ID_FIELD_KEY, request.tenantId());

            try {
                // Create a mock GetResponse with cached data
                String responseJson = String.format(
                    "{\"_index\":\"%s\",\"_id\":\"%s\",\"found\":true,\"_source\":%s}",
                    request.index(),
                    request.id(),
                    OBJECT_MAPPER.writeValueAsString(modifiedSource)
                );

                return GetDataObjectResponse.builder()
                    .id(request.id())
                    .parser(SdkClientUtils.createParser(responseJson))
                    .source(modifiedSource)
                    .build();
            } catch (Exception e) {
                log.error("Failed to create response from cached global resource", e);
                return null;
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        stopGlobalResourcesCacheScheduler();
        if (openSearchAsyncClient != null && openSearchAsyncClient._transport() != null) {
            openSearchAsyncClient._transport().close();
        }
    }
}
