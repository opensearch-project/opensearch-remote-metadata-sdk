/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static org.opensearch.remote.metadata.common.SdkClientUtils.unwrapAndConvertToException;

/**
 * A concrete client object which delegates calls to a {@link SdkClientDelegate} implementation after validating the tenant id.
 */
public class SdkClient {

    private final SdkClientDelegate delegate;
    private final Executor defaultExecutor;
    private final Boolean isMultiTenancyEnabled;
    private final String globalTenantId;
    protected static final String NO_PERMISSION_TO_OPERATE_GLOBAL_RESOURCE = "You don't have permission to operate on this resource!";

    /**
     * Instantiate this client with the {@link ForkJoinPool#commonPool()} as the default executor
     * @param delegate The client implementation to delegate calls to
     * @param multiTenancy whether multiTenancy is enabled
     * @param globalTenantId global tenant id initialized in cluster setting.
     */
    public SdkClient(SdkClientDelegate delegate, Boolean multiTenancy, String globalTenantId) {
        this(delegate, ForkJoinPool.commonPool(), multiTenancy, globalTenantId);
    }

    /**
     * Instantiate this client with a default Executor
     * @param delegate The client implementation to delegate calls to
     * @param defaultExecutor A default executor to use for asynchronous execution unless otherwise specified
     * @param multiTenancy whether multiTenancy is enabled
     * @param globalTenantId global tenant id for global resource.
     */
    public SdkClient(SdkClientDelegate delegate, Executor defaultExecutor, Boolean multiTenancy, String globalTenantId) {
        this.delegate = delegate;
        this.defaultExecutor = defaultExecutor;
        this.isMultiTenancyEnabled = multiTenancy;
        this.globalTenantId = globalTenantId;
    }

    /**
     * Create/Put/Index a data object/document into a table/index.
     * @param request A request encapsulating the data object to store
     * @param executor the executor to use for asynchronous execution
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<PutDataObjectResponse> putDataObjectAsync(PutDataObjectRequest request, Executor executor) {
        validateTenantId(request.tenantId());
        return delegate.putDataObjectAsync(request, executor, isMultiTenancyEnabled);
    }

    /**
     * Create/Put/Index a data object/document into a table/index using the default executor.
     * @param request A request encapsulating the data object to store
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<PutDataObjectResponse> putDataObjectAsync(PutDataObjectRequest request) {
        return putDataObjectAsync(request, defaultExecutor);
    }

    /**
     * Create/Put/Index a data object/document into a table/index.
     * @param request A request encapsulating the data object to store
     * @return A response on success. Throws unchecked exceptions or {@link OpenSearchException} wrapping the cause on checked exception.
     */
    public PutDataObjectResponse putDataObject(PutDataObjectRequest request) {
        try {
            return putDataObjectAsync(request).toCompletableFuture().join();
        } catch (CompletionException e) {
            throw ExceptionsHelper.convertToRuntime(unwrapAndConvertToException(e));
        }
    }

    /**
     * Read/Get a data object/document from a table/index using the default executor.
     *
     * @param request  A request identifying the data object to retrieve
     * @param executor the executor to use for asynchronous execution
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<GetDataObjectResponse> getDataObjectAsync(GetDataObjectRequest request, Executor executor) {
        return delegate.getDataObjectAsync(request, executor, isMultiTenancyEnabled);
    }

    /**
     * Read/Get a data object/document from a table/index.
     *
     * @param request A request identifying the data object to retrieve
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<GetDataObjectResponse> getDataObjectAsync(GetDataObjectRequest request) {
        validateTenantId(request.tenantId());
        return getDataObjectAsync(request, defaultExecutor);
    }

    /**
     * Read/Get a data object/document from a table/index.
     * @param request A request identifying the data object to retrieve
     * @return A response on success. Throws unchecked exceptions or {@link OpenSearchException} wrapping the cause on checked exception.
     */
    public GetDataObjectResponse getDataObject(GetDataObjectRequest request) {
        try {
            return getDataObjectAsync(request).toCompletableFuture().join();
        } catch (CompletionException e) {
            throw ExceptionsHelper.convertToRuntime(unwrapAndConvertToException(e));
        }
    }

    /**
     * Update a data object/document in a table/index.
     *
     * @param request  A request identifying the data object to update
     * @param executor the executor to use for asynchronous execution
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<UpdateDataObjectResponse> updateDataObjectAsync(UpdateDataObjectRequest request, Executor executor) {
        validateTenantId(request.tenantId());
        return delegate.updateDataObjectAsync(request, executor, isMultiTenancyEnabled);
    }

    /**
     * Update a data object/document in a table/index using the default executor.
     *
     * @param request A request identifying the data object to update
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<UpdateDataObjectResponse> updateDataObjectAsync(UpdateDataObjectRequest request) {
        return updateDataObjectAsync(request, defaultExecutor);
    }

    /**
     * Update a data object/document in a table/index.
     * @param request A request identifying the data object to update
     * @return A response on success. Throws unchecked exceptions or {@link OpenSearchException} wrapping the cause on checked exception.
     */
    public UpdateDataObjectResponse updateDataObject(UpdateDataObjectRequest request) {
        try {
            return updateDataObjectAsync(request).toCompletableFuture().join();
        } catch (CompletionException e) {
            throw ExceptionsHelper.convertToRuntime(unwrapAndConvertToException(e));
        }
    }

    /**
     * Delete a data object/document from a table/index.
     *
     * @param request  A request identifying the data object to delete
     * @param executor the executor to use for asynchronous execution
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<DeleteDataObjectResponse> deleteDataObjectAsync(DeleteDataObjectRequest request, Executor executor) {
        validateTenantId(request.tenantId());
        return delegate.deleteDataObjectAsync(request, executor, isMultiTenancyEnabled);
    }

    /**
     * Delete a data object/document from a table/index using the default executor.
     *
     * @param request A request identifying the data object to delete
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<DeleteDataObjectResponse> deleteDataObjectAsync(DeleteDataObjectRequest request) {
        return deleteDataObjectAsync(request, defaultExecutor);
    }

    /**
     * Delete a data object/document from a table/index.
     * @param request A request identifying the data object to delete
     * @return A response on success. Throws unchecked exceptions or {@link OpenSearchException} wrapping the cause on checked exception.
     */
    public DeleteDataObjectResponse deleteDataObject(DeleteDataObjectRequest request) {
        try {
            return deleteDataObjectAsync(request).toCompletableFuture().join();
        } catch (CompletionException e) {
            throw ExceptionsHelper.convertToRuntime(unwrapAndConvertToException(e));
        }
    }

    /**
     * Perform a bulk request for multiple data objects/documents in potentially multiple tables/indices.
     *
     * @param request A request identifying the bulk requests to execute
     * @param executor the executor to use for asynchronous execution
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<BulkDataObjectResponse> bulkDataObjectAsync(BulkDataObjectRequest request, Executor executor) {
        validateTenantIds(request.requests());
        return delegate.bulkDataObjectAsync(request, executor, isMultiTenancyEnabled);
    }

    /**
     * Perform a bulk request for multiple data objects/documents in potentially multiple tables/indices using the default executor.
     *
     * @param request A request identifying the bulk requests to execute
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<BulkDataObjectResponse> bulkDataObjectAsync(BulkDataObjectRequest request) {
        return bulkDataObjectAsync(request, defaultExecutor);
    }

    /**
     * Perform a bulk request for multiple data objects/documents in potentially multiple tables/indices.
     *
     * @param request A request identifying the bulk requests to execute
     * @return A response on success. Throws unchecked exceptions or {@link OpenSearchException} wrapping the cause on checked exception.
     */
    public BulkDataObjectResponse bulkDataObject(BulkDataObjectRequest request) {
        try {
            return bulkDataObjectAsync(request).toCompletableFuture().join();
        } catch (CompletionException e) {
            throw ExceptionsHelper.convertToRuntime(unwrapAndConvertToException(e));
        }
    }

    /**
     * Search for data objects/documents in a table/index.
     *
     * @param request  A request identifying the data objects to search for
     * @param executor the executor to use for asynchronous execution
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<SearchDataObjectResponse> searchDataObjectAsync(SearchDataObjectRequest request, Executor executor) {
        validateTenantId(request.tenantId());
        return delegate.searchDataObjectAsync(request, executor, isMultiTenancyEnabled);
    }

    /**
     * Search for data objects/documents in a table/index using the default executor.
     *
     * @param request A request identifying the data objects to search for
     * @return A completion stage encapsulating the response or exception
     */
    public CompletionStage<SearchDataObjectResponse> searchDataObjectAsync(SearchDataObjectRequest request) {
        return searchDataObjectAsync(request, defaultExecutor);
    }

    /**
     * Search for data objects/documents in a table/index.
     * @param request A request identifying the data objects to search for
     * @return A response on success. Throws unchecked exceptions or {@link OpenSearchException} wrapping the cause on checked exception.
     */
    public SearchDataObjectResponse searchDataObject(SearchDataObjectRequest request) {
        try {
            return searchDataObjectAsync(request).toCompletableFuture().join();
        } catch (CompletionException e) {
            throw ExceptionsHelper.convertToRuntime(unwrapAndConvertToException(e));
        }
    }

    /**
     * Get the delegate client implementation.
     * @return the delegate implementation
     */
    public SdkClientDelegate getDelegate() {
        return delegate;
    }

    /**
     * Throw exception if tenantId is null and multitenancy is enabled
     * @param tenantId The tenantId from the request
     */
    private void validateTenantId(String tenantId) {
        if (Boolean.TRUE.equals(isMultiTenancyEnabled)) {
            if (Strings.isNullOrEmpty(tenantId)) {
                throw new IllegalArgumentException("A tenant ID is required when multitenancy is enabled.");
            } else if (tenantId.equals(globalTenantId)) {
                throw new OpenSearchStatusException(NO_PERMISSION_TO_OPERATE_GLOBAL_RESOURCE, RestStatus.FORBIDDEN);
            }
        }
    }

    /**
     * Throw exception if tenantId is null for any bulk request and multitenancy is enabled
     * @param tenantId The tenantId from the request
     */
    private void validateTenantIds(List<DataObjectRequest> requests) {
        if (Boolean.TRUE.equals(isMultiTenancyEnabled)
            && requests.stream().map(DataObjectRequest::tenantId).anyMatch(Strings::isNullOrEmpty)) {
            throw new IllegalArgumentException("A tenant ID is required for every bulk request when multitenancy is enabled.");
        }
    }

    /**
     * This method is to check if a given resource index and id is global resource or not.
     * This is useful for invokers in case they need perform different operations based on
     * resource type. It delegates to the underlying implementation.
     * @param index The index/table name.
     * @param id The resource id.
     * @return If the resource is global or not.
     */
    public CompletionStage<Boolean> isGlobalResource(String index, String id) {
        return delegate.isGlobalResource(index, id, defaultExecutor, isMultiTenancyEnabled);
    }
}
