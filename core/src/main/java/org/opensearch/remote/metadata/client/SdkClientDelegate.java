/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.remote.metadata.common.CommonValue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * An interface required by a client implementation, which is wrapped in the SdkClient
 */
public interface SdkClientDelegate extends AutoCloseable {

    /**
     * Which metadata type the implementation supports
     * @param metadataType A matching value for the {@link CommonValue#REMOTE_METADATA_TYPE_KEY}
     * @return true if this implementation supports that metadata type
     */
    boolean supportsMetadataType(String metadataType);

    /**
     * Initialize this client
     * @param metadataSettings The map of metadata settings needed for initialization
     */
    void initialize(Map<String, String> metadataSettings);

    /**
     * Create/Put/Index a data object/document into a table/index.
     * @param request A request encapsulating the data object to store
     * @param executor the executor to use for asynchronous execution
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @return A completion stage encapsulating the response or exception
     */
    CompletionStage<PutDataObjectResponse> putDataObjectAsync(
        PutDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    );

    /**
     * Read/Get a data object/document from a table/index.
     *
     * @param request  A request identifying the data object to retrieve
     * @param executor the executor to use for asynchronous execution
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @return A completion stage encapsulating the response or exception
     */
    CompletionStage<GetDataObjectResponse> getDataObjectAsync(
        GetDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    );

    /**
     * Update a data object/document in a table/index.
     *
     * @param request  A request identifying the data object to update
     * @param executor the executor to use for asynchronous execution
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @return A completion stage encapsulating the response or exception
     */
    CompletionStage<UpdateDataObjectResponse> updateDataObjectAsync(
        UpdateDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    );

    /**
     * Delete a data object/document from a table/index.
     *
     * @param request  A request identifying the data object to delete
     * @param executor the executor to use for asynchronous execution
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @return A completion stage encapsulating the response or exception
     */
    CompletionStage<DeleteDataObjectResponse> deleteDataObjectAsync(
        DeleteDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    );

    /**
     * Perform a bulk request for multiple data objects/documents in potentially multiple tables/indices.
     *
     * @param request  A request identifying the requests to process in bulk
     * @param executor the executor to use for asynchronous execution
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @return A completion stage encapsulating the response or exception
     */
    CompletionStage<BulkDataObjectResponse> bulkDataObjectAsync(
        BulkDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    );

    /**
     * Search for data objects/documents in a table/index.
     *
     * @param request  A request identifying the data objects to search for
     * @param executor the executor to use for asynchronous execution
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @return A completion stage encapsulating the response or exception
     */
    CompletionStage<SearchDataObjectResponse> searchDataObjectAsync(
        SearchDataObjectRequest request,
        Executor executor,
        Boolean isMultiTenancyEnabled
    );

    /**
     * This method is to check if a given resource index and id is global resource or not.
     * This is useful for invokers in case they need perform different operations based on
     * resource type. It's false by default since for most cases, override this method if
     * otherwise.
     * @param index The index/table name.
     * @param id The resource id.
     * @return If the resource is global or not.
     */
    default CompletionStage<Boolean> isGlobalResource(String index, String id) {
        return CompletableFuture.completedFuture(false);
    }
}
