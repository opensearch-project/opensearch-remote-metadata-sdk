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

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
/**
 * A class abstracting an OpenSearch IndexRequest
 */
public class PutDataObjectRequest extends DataObjectRequest {

    private final boolean overwriteIfExists;
    private final ToXContentObject dataObject;
    private final RefreshPolicy refreshPolicy;
    private final Long ifSeqNo;
    private final Long ifPrimaryTerm;
    private final TimeValue timeout;

    /**
     * Instantiate this request with an index and data object.
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table and the data object may be referred to as an item.
     * @param index the index location to put the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param overwriteIfExists whether to overwrite the document if it exists (update)
     * @param dataObject the data object
     * @param refreshPolicy the refresh policy to match, by default set as IMMEDIATE
     * @param ifSeqNo the sequence number to match or null if not required
     * @param ifPrimaryTerm the primary term to match or null if not required
     * @param timeout the timeout to match or null if not required
     */
    public PutDataObjectRequest(String index, String id, String tenantId, boolean overwriteIfExists, ToXContentObject dataObject, RefreshPolicy refreshPolicy, Long ifSeqNo, Long ifPrimaryTerm, TimeValue timeout) {
        super(index, id, tenantId);
        this.overwriteIfExists = overwriteIfExists;
        this.dataObject = dataObject;
        this.refreshPolicy = refreshPolicy;
        this.ifSeqNo = ifSeqNo;
        this.ifPrimaryTerm = ifPrimaryTerm;
        this.timeout = timeout;
    }

    /**
     * Returns whether to overwrite an existing document (upsert)
     * @return true if this request should overwrite
     */
    public boolean overwriteIfExists() {
        return this.overwriteIfExists;
    }

    /**
     * Returns the refresh policy for this request
     * @return the refreshPolicy
     */
    public RefreshPolicy refreshPolicy() { return refreshPolicy; }

    /**
     * Returns the sequence number to match, or null if no match required
     * @return the ifSeqNo
     */
    public Long ifSeqNo() {
        return ifSeqNo;
    }

    /**
     * Returns the primary term to match, or null if no match required
     * @return the ifPrimaryTerm
     */
    public Long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }

    /**
     * Returns the timeout value for this request, or null if not specified
     * @return the timeout
     */
    public TimeValue timeout() { return timeout; }

    /**
     * Returns the data object
     * @return the data object
     */
    public ToXContentObject dataObject() {
        return this.dataObject;
    }

    @Override
    public boolean isWriteRequest() {
        return true;
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
    public static class Builder extends DataObjectRequest.Builder<Builder> {
        private boolean overwriteIfExists = true;
        private ToXContentObject dataObject = null;
        private Long ifSeqNo = null;
        private Long ifPrimaryTerm = null;
        private TimeValue timeout = null;
        private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

        /**
         * Sets the refresh policy for this request
         * refresh policy may not be relevant on non-OpenSearch data stores.
         * @param refreshPolicy the refresh policy to use
         * @return the updated builder
         */
        public Builder refreshPolicy(RefreshPolicy refreshPolicy) {
            this.refreshPolicy = refreshPolicy;
            return this;
        }

        /**
         * Only perform this index request if the document's modification was assigned the given
         * sequence number. Must be used in combination with {@link #ifPrimaryTerm(long)}
         * <p>
         * Sequence number may be represented by a different document versioning key on non-OpenSearch data stores.
         * @param seqNo the sequence number
         * @return the updated builder
         */
        public PutDataObjectRequest.Builder ifSeqNo(long seqNo) {
            if (seqNo < 0 && seqNo != UNASSIGNED_SEQ_NO) {
                throw new IllegalArgumentException("sequence numbers must be non negative. got [" + seqNo + "].");
            }
            this.ifSeqNo = seqNo;
            return this;
        }

        /**
         * Only performs this index request if the document's last modification was assigned the given
         * primary term. Must be used in combination with {@link #ifSeqNo(long)}
         * <p>
         * Primary term may not be relevant on non-OpenSearch data stores.
         * @param term the primary term
         * @return the updated builder
         */
        public Builder ifPrimaryTerm(long term) {
            if (term < 0) {
                throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
            }
            this.ifPrimaryTerm = term;
            return this;
        }

        /**
         * Sets the timeout for this request
         * @param timeout the timeout value
         * @return the updated builder
         */
        public Builder timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

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
            return new PutDataObjectRequest(this.index, this.id, this.tenantId, this.overwriteIfExists, this.dataObject, this.refreshPolicy, this.ifSeqNo, this.ifPrimaryTerm, this.timeout);
        }
    }
}
