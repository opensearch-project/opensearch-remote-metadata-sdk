/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentObject;

import java.util.Map;

/**
 * A class abstracting an OpenSearch IndexRequest
 */
public class PutDataObjectRequest extends WriteDataObjectRequest<PutDataObjectRequest> {

    private final boolean overwriteIfExists;
    private final ToXContentObject dataObject;

    /**
     * Instantiate this request with an index and data object.
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table and the data object may be referred to as an item.
     * @param index the index location to put the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param ifSeqNo the sequence number to match or null if not required
     * @param ifPrimaryTerm the primary term to match or null if not required
     * @param refreshPolicy when should the written data be refreshed. May not be applicable on all clients. Defaults to {@code IMMEDIATE}.
     * @param timeout A timeout to wait if the index operation can't be performed immediately. May not be applicable on all clients. Defaults to {@code 1m}.
     * @param overwriteIfExists whether to overwrite the document if it exists (update)
     * @param dataObject the data object
     * @param cmkRoleArn the cmk arn role to encrypt/decrypt
     * @param assumeRoleArn A role to assume for cmk
     */
    public PutDataObjectRequest(
        String index,
        String id,
        String tenantId,
        Long ifSeqNo,
        Long ifPrimaryTerm,
        RefreshPolicy refreshPolicy,
        TimeValue timeout,
        boolean overwriteIfExists,
        ToXContentObject dataObject,
        String cmkRoleArn,
        String assumeRoleArn
    ) {
        super(index, id, tenantId, ifSeqNo, ifPrimaryTerm, refreshPolicy, timeout, !overwriteIfExists, cmkRoleArn, assumeRoleArn);
        this.overwriteIfExists = overwriteIfExists;
        this.dataObject = dataObject;
    }

    /**
     * Returns whether to overwrite an existing document (upsert)
     * @return true if this request should overwrite
     */
    public boolean overwriteIfExists() {
        return this.overwriteIfExists;
    }

    /**
     * Returns the data object
     * @return the data object
     */
    public ToXContentObject dataObject() {
        return this.dataObject;
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
    public static class Builder extends WriteDataObjectRequest.Builder<Builder> {
        private boolean overwriteIfExists = true;
        private ToXContentObject dataObject = null;

        /**
         * Specify whether to overwrite an existing document/item (upsert). True by default.
         * @param overwriteIfExists whether to overwrite an existing document/item
         * @return the updated builder
         */
        public Builder overwriteIfExists(boolean overwriteIfExists) {
            this.overwriteIfExists = overwriteIfExists;
            return this;
        }

        /**
         * Add a data object to this builder
         * @param dataObject the data object
         * @return the updated builder
         */
        public Builder dataObject(ToXContentObject dataObject) {
            this.dataObject = dataObject;
            return this;
        }

        /**
         * Add a data object as a map to this builder
         * @param dataObjectMap the data object as a map of fields
         * @return the updated builder
         */
        public Builder dataObject(Map<String, Object> dataObjectMap) {
            this.dataObject = (builder, params) -> builder.map(dataObjectMap);
            return this;
        }

        /**
         * Builds the request
         * @return A {@link PutDataObjectRequest}
         */
        public PutDataObjectRequest build() {
            WriteDataObjectRequest.validateSeqNoAndPrimaryTerm(
                this.ifSeqNo,
                this.ifPrimaryTerm,
                // createOperation = true when overwriteIfExists is false
                !this.overwriteIfExists
            );
            return new PutDataObjectRequest(
                this.index,
                this.id,
                this.tenantId,
                this.ifSeqNo,
                this.ifPrimaryTerm,
                this.refreshPolicy,
                this.timeout,
                this.overwriteIfExists,
                this.dataObject,
                this.cmkRoleArn,
                this.assumeRoleArn
            );
        }
    }
}
