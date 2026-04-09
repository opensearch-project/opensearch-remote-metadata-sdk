/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.common.Nullable;

/**
 * A superclass for common fields in Data Object Request classes
 */
public abstract class DataObjectRequest {

    private String index;
    private final String id;
    private String tenantId;
    private final String cmkRoleArn;
    private final String assumeRoleArn;
    private final @Nullable String routing;

    /**
     * Instantiate this request with an index and id.
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table and the id may be referred to as a primary key.
     * @param index the index location to delete the object
     * @param id the document id
     * @param tenantId the tenant id
     */
    protected DataObjectRequest(String index, String id, String tenantId) {
        this(index, id, tenantId, null, null, null);
    }

    /**
     * Overloaded constructor allowing optional CMK role ARN.
     * @param index the index location to delete the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param cmkRoleArn optional CMK role ARN (nullable)
     */
    protected DataObjectRequest(String index, String id, String tenantId, String cmkRoleArn) {
        this(index, id, tenantId, cmkRoleArn, null, null);
    }

    /**
     * Overloaded constructor allowing optional CMK role ARN and assume role ARN.
     * @param index the index location to delete the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param cmkRoleArn optional CMK role ARN (nullable)
     * @param assumeRoleArn optional role ARN to assume (nullable)
     */
    protected DataObjectRequest(String index, String id, String tenantId, String cmkRoleArn, String assumeRoleArn) {
        this(index, id, tenantId, cmkRoleArn, assumeRoleArn, null);
    }

    /**
     * Overloaded constructor with all fields including routing.
     * @param index the index location to delete the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param cmkRoleArn optional CMK role ARN (nullable)
     * @param assumeRoleArn optional role ARN to assume (nullable)
     * @param routing optional routing value for shard selection (nullable)
     */
    protected DataObjectRequest(
        String index,
        String id,
        String tenantId,
        String cmkRoleArn,
        String assumeRoleArn,
        @Nullable String routing
    ) {
        this.index = index;
        this.id = id;
        this.tenantId = tenantId;
        this.cmkRoleArn = cmkRoleArn;
        this.assumeRoleArn = assumeRoleArn;
        this.routing = routing;
    }

    /**
     * Returns the index
     * @return the index
     */
    public String index() {
        return this.index;
    }

    /**
     * Sets the index
     * @param index The new index to set
     */
    public void index(String index) {
        this.index = index;
    }

    /**
     * Returns the document id
     * @return the id
     */
    public String id() {
        return this.id;
    }

    /**
     * Returns the tenant id
     * @return the tenantId
     */
    public String tenantId() {
        return this.tenantId;
    }

    /**
     * Sets the tenant id
     * @param tenantId The new tenant id to set
     */
    public void tenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /**
     * Returns the optional CMK role ARN (may be null).
     * @return cmkRoleArn or null if not set
     */
    public String cmkRoleArn() {
        return this.cmkRoleArn;
    }

    /**
     * Returns the optional assume role for CMK (may be null).
     * @return assumeRoleArn or null if not set
     */
    public String assumeRoleArn() {
        return this.assumeRoleArn;
    }

    /**
     * Returns the routing value for shard selection. May not be applicable on all clients.
     * @return the routing value or null if not set
     */
    public @Nullable String routing() {
        return this.routing;
    }

    /**
     * Returns whether the subclass can be used in a {@link BulkDataObjectRequest}
     * @return whether the subclass is a write request
     */
    public abstract boolean isWriteRequest();

    /**
     * Superclass for common fields in subclass builders
     *
     * @param <T> The type of the subclass builder
     */
    public static class Builder<T extends Builder<T>> {
        protected String index = null;
        protected String id = null;
        protected String tenantId = null;
        protected String cmkRoleArn = null;
        protected String assumeRoleArn = null;
        protected String routing = null;

        /**
         * Empty constructor to initialize
         */
        protected Builder() {}

        /**
         * Add an index to this builder
         * @param index the index to put the object
         * @return the updated builder
         */
        public T index(String index) {
            this.index = index;
            return self();
        }

        /**
         * Add an id to this builder
         * @param id the document id
         * @return the updated builder
         */
        public T id(String id) {
            this.id = id;
            return self();
        }

        /**
         * Add a tenant id to this builder
         * @param tenantId the tenant id
         * @return the updated builder
         */
        public T tenantId(String tenantId) {
            this.tenantId = tenantId;
            return self();
        }

        /**
         * Add an optional CMK role ARN to this builder (nullable).
         * @param cmkRoleArn CMK role ARN or null
         * @return the updated builder
         */
        public T cmkRoleArn(String cmkRoleArn) {
            this.cmkRoleArn = cmkRoleArn;
            return self();
        }

        /**
         * Add an optional assume role ARN for cmk to this builder (nullable).
         * @param assumeRoleArn assume role ARN for cmk or null
         * @return the updated builder
         */
        public T assumeRoleArn(String assumeRoleArn) {
            this.assumeRoleArn = assumeRoleArn;
            return self();
        }

        /**
         * Add a routing value for shard selection. May not be applicable on all clients.
         * @param routing the routing value
         * @return the updated builder
         */
        public T routing(String routing) {
            this.routing = routing;
            return self();
        }

        /**
         * Returns this builder as the parameterized type.
         * @return the builder cast to its correct type
         */
        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }
    }
}
