/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_GLOBAL_RESOURCE_CACHE_TTL_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_GLOBAL_TENANT_ID_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;

/**
 * Superclass abstracting privileged action execution
 */
public abstract class AbstractSdkClient implements SdkClientDelegate {

    // TENANT_ID hash key requires non-null value
    protected static final String DEFAULT_TENANT = "DEFAULT_TENANT";
    protected static final TimeValue DEFAULT_GLOBAL_RESOURCE_CACHE_TTL = TimeValue.timeValueMillis(5 * 60 * 1000);
    protected static final Map<String, Tuple<GetDataObjectResponse, Long>> GLOBAL_RESOURCES_CACHE = new ConcurrentHashMap<>();
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected String tenantIdField;
    protected String globalTenantId;
    // global resource cache TTL in milliseconds, should be positive numbers, 0 means no cache.
    protected TimeValue globalResourceCacheTTL;

    protected String remoteMetadataType;
    protected String remoteMetadataEndpoint;
    protected String region;
    protected String serviceName;

    @Override
    public void initialize(Map<String, String> metadataSettings) {
        this.tenantIdField = metadataSettings.get(TENANT_ID_FIELD_KEY);

        this.remoteMetadataType = metadataSettings.get(REMOTE_METADATA_TYPE_KEY);
        this.remoteMetadataEndpoint = metadataSettings.get(REMOTE_METADATA_ENDPOINT_KEY);
        this.region = metadataSettings.get(REMOTE_METADATA_REGION_KEY);
        this.serviceName = metadataSettings.get(REMOTE_METADATA_SERVICE_NAME_KEY);

        globalTenantId = metadataSettings.get(REMOTE_METADATA_GLOBAL_TENANT_ID_KEY);
        globalResourceCacheTTL = Optional.ofNullable(metadataSettings.get(REMOTE_METADATA_GLOBAL_RESOURCE_CACHE_TTL_KEY))
            .map(x -> TimeValue.parseTimeValue(x, DEFAULT_GLOBAL_RESOURCE_CACHE_TTL, REMOTE_METADATA_GLOBAL_RESOURCE_CACHE_TTL_KEY))
            .filter(x -> x.getMillis() >= 0)
            .orElse(DEFAULT_GLOBAL_RESOURCE_CACHE_TTL);
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

    /**
     * Determine if we should use the user-provided document id
     * @param id the document id
     * @return true if the id is not null
     * @throws IllegalArgumentException if the id is an empty string
     */
    protected boolean shouldUseId(String id) {
        if ("".equals(id)) {
            throw new IllegalArgumentException("if _id is specified it must not be empty");
        }
        return id != null;
    }

    /**
     * Handle the result queried either from remote AOS cluster or local cluster which returns OpenSearch document results.
     * @param request the original request.
     * @param dataFetched data fetched with original request.
     * @return {@link CompletionStage<GetDataObjectResponse>} which either is the user's resource or global resource been added to cache then replaced the tenant id to which from request.
     */
    protected CompletionStage<GetDataObjectResponse> handleOSDocumentBasedResponse(
        GetDataObjectRequest request,
        CompletionStage<GetDataObjectResponse> dataFetched
    ) {
        return dataFetched.thenCompose(response -> {
            if (response == null || response.getResponse() == null || !response.getResponse().isExists()) {
                // Resource not found case
                return CompletableFuture.completedFuture(response);
            }

            if (!isGlobalResource(response)) {
                // return if it's user's resource
                return CompletableFuture.completedFuture(response);
            }
            return addToGlobalResourceCache(request, dataFetched);
        });
    }

    /**
     * Add the global resource to cache.
     * @param request origin request.
     * @param dataFetchedWithGlobalTenantId data fetched with global tenant id, which means this resource is a global resource.
     * @return {@link CompletionStage<GetDataObjectResponse>} The response been added to cache and then replaced the tenant_id to request's tenant id.
     */
    protected CompletionStage<GetDataObjectResponse> addToGlobalResourceCache(
        GetDataObjectRequest request,
        CompletionStage<GetDataObjectResponse> dataFetchedWithGlobalTenantId
    ) {
        return dataFetchedWithGlobalTenantId.thenCompose(res -> {
            if (globalResourceCacheTTL.getMillis() == 0) {
                return CompletableFuture.completedFuture(replaceGlobalTenantId(request, res));
            }
            GLOBAL_RESOURCES_CACHE.put(buildGlobalCacheKey(request.index(), request.id()), new Tuple<>(res, System.currentTimeMillis()));
            return CompletableFuture.completedFuture(getGlobalResourceDataFromCache(request));
        });
    }

    /**
     * Read global resource from cache.
     * @param request origin get request.
     * @return {@link GetDataObjectResponse} The response cached in cache and replaced with request tenant id.
     */
    protected GetDataObjectResponse getGlobalResourceDataFromCache(GetDataObjectRequest request) {
        String checkingKey = buildGlobalCacheKey(request.index(), request.id());
        long currentTime = System.currentTimeMillis();
        Tuple<GetDataObjectResponse, Long> tuple = GLOBAL_RESOURCES_CACHE.computeIfPresent(checkingKey, (key, value) -> {
            if (currentTime - value.v2() > globalResourceCacheTTL.getMillis()) {
                return null; // If expired, return null to remove the entry
            }
            return value; // Keep the entry
        });
        // If we have a value, replace the tenant id in the global resource response to actual tenant id
        // to bypass the validation of the resources e.g.:
        // https://github.com/opensearch-project/ml-commons/blob/main/ml-algorithms/src/main/java/org/opensearch/ml/engine/algorithms/agent/MLAgentExecutor.java#L206
        return tuple != null ? replaceGlobalTenantId(request, tuple.v1()) : null;
    }

    private GetDataObjectResponse replaceGlobalTenantId(GetDataObjectRequest request, GetDataObjectResponse response) {
        response.source().put(TENANT_ID_FIELD_KEY, request.tenantId());
        GetResponse getResponse = response.getResponse();
        if (getResponse == null) {
            throw new OpenSearchStatusException(
                "Cached response is null, please check configuration with system admin!",
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }
        String responseStr = getResponse.toString()
            .replaceAll(globalTenantId, Optional.ofNullable(request.tenantId()).orElse(DEFAULT_TENANT));
        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                responseStr
            );
            return GetDataObjectResponse.builder().id(request.id()).parser(parser).source(response.source()).build();
        } catch (IOException e) {
            throw new OpenSearchStatusException(
                "Failed to parse cached global response, please check configuration with system admin!",
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            );
        }
    }

    /**
     * Building the global cache key, which is a combination of index_name:resource_id.
     * @param index index name / table name.
     * @param id resource id.
     * @return {@link String} built resource cache key.
     */
    protected String buildGlobalCacheKey(String index, String id) {
        return index + ":" + id;
    }

    /**
     * Determine if the resource is global resource by given a {@link GetDataObjectResponse}.
     * @param response {@link GetDataObjectResponse} the response read from storage.
     * @return {@link Boolean} to indicate if it's a global resource.
     */
    protected boolean isGlobalResource(GetDataObjectResponse response) {
        return Optional.ofNullable(response.getResponse())
            .map(GetResponse::getSourceAsMap)
            .map(x -> x.get(TENANT_ID_FIELD_KEY))
            .map(y -> Objects.equals(y, globalTenantId))
            .orElse(false);
    }

    // Visible for testing
    public void setGlobalTenantId(String globalTenantId) {
        this.globalTenantId = globalTenantId;
    }

    // visible for testing
    public void setGlobalResourceCacheTTL(TimeValue globalResourceCacheTTL) {
        this.globalResourceCacheTTL = globalResourceCacheTTL;
    }
}
