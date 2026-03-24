/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * A class abstracting an OpenSearch SearchRequest
 */
public class SearchDataObjectRequest {

    private final String[] indices;
    private final String tenantId;
    private final SearchSourceBuilder searchSourceBuilder;
    private final boolean searchRemoteReplica;

    /**
     * Instantiate this request with an optional list of indices and search source
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table
     * Defaults to searching the remote replica.
     *
     * @param indices the indices to search for the object
     * @param tenantId the tenant id
     * @param searchSourceBuilder the search body containing the query
     */
    public SearchDataObjectRequest(String[] indices, String tenantId, SearchSourceBuilder searchSourceBuilder) {
        this(indices, tenantId, searchSourceBuilder, true);
    }

    /**
     * Instantiate this request with an optional list of indices, search source, and remote replica flag.
     *
     * @param indices the indices to search for the object
     * @param tenantId the tenant id
     * @param searchSourceBuilder the search body containing the query
     * @param searchRemoteReplica when true (default), delegates search to a remote search-optimized replica
     *                            (e.g., AOS/AOSS via zero-ETL). When false, searches the primary data store directly.
     */
    public SearchDataObjectRequest(
        String[] indices,
        String tenantId,
        SearchSourceBuilder searchSourceBuilder,
        boolean searchRemoteReplica
    ) {
        this.indices = indices;
        this.tenantId = tenantId;
        this.searchSourceBuilder = searchSourceBuilder;
        this.searchRemoteReplica = searchRemoteReplica;
    }

    /**
     * Returns the indices
     * @return the indices
     */
    public String[] indices() {
        return this.indices;
    }

    /**
     * Returns the tenant id
     * @return the tenantId
     */
    public String tenantId() {
        return this.tenantId;
    }

    /**
     * Returns the builder for searching
     * @return the SearchSourceBuilder
     */
    public SearchSourceBuilder searchSourceBuilder() {
        return this.searchSourceBuilder;
    }

    /**
     * Returns whether to search the remote replica or the primary data store directly.
     * @return true if searching the remote replica (default), false for primary store
     */
    public boolean searchRemoteReplica() {
        return this.searchRemoteReplica;
    }

    /**
     * Instantiate a builder for this object
     * @return a builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Class for constructing a Builder for this Request Object
     */
    public static class Builder {
        private String[] indices = null;
        private String tenantId = null;
        private SearchSourceBuilder searchSourceBuilder;
        private boolean searchRemoteReplica = true;

        /**
         * Empty Constructor for the Builder object
         */
        private Builder() {}

        /**
         * Add a indices to this builder
         * @param indices the index to put the object
         * @return the updated builder
         */
        public Builder indices(String... indices) {
            this.indices = indices;
            return this;
        }

        /**
         * Add a tenant ID to this builder
         * @param tenantId the tenant id
         * @return the updated builder
         */
        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        /**
         * Add a SearchSourceBuilder to this builder
         * @param searchSourceBuilder the searchSourceBuilder
         * @return the updated builder
         */
        public Builder searchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
            this.searchSourceBuilder = searchSourceBuilder;
            return this;
        }

        /**
         * Set whether to search the remote replica or the primary data store directly.
         * Default is true (existing behavior — delegates to remote replica).
         * @param searchRemoteReplica false to search primary store directly
         * @return the updated builder
         */
        public Builder searchRemoteReplica(boolean searchRemoteReplica) {
            this.searchRemoteReplica = searchRemoteReplica;
            return this;
        }

        /**
         * Builds the request
         * @return A {@link SearchDataObjectRequest}
         */
        public SearchDataObjectRequest build() {
            return new SearchDataObjectRequest(this.indices, this.tenantId, this.searchSourceBuilder, this.searchRemoteReplica);
        }
    }
}
