/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A class abstracting an OpenSearch UpdateRequest
 */
public class UpdateDataObjectRequest extends WriteDataObjectRequest {

    private final int retryOnConflict;
    private final ToXContentObject dataObject;

    /**
     * Instantiate this request with an index and data object.
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table and the data object may be referred to as an item.
     * @param index the index location to update the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param ifSeqNo the sequence number to match or null if not required
     * @param ifPrimaryTerm the primary term to match or null if not required
     * @param retryOnConflict number of times to retry an update if a version conflict exists
     * @param dataObject the data object
     */
    public UpdateDataObjectRequest(
        String index,
        String id,
        String tenantId,
        Long ifSeqNo,
        Long ifPrimaryTerm,
        int retryOnConflict,
        ToXContentObject dataObject
    ) {
        super(index, id, tenantId, ifSeqNo, ifPrimaryTerm, false);
        this.retryOnConflict = retryOnConflict;
        this.dataObject = dataObject;
    }

    /**
     * Returns the number of retries on version conflict
     * @return the number of retries
     */
    public int retryOnConflict() {
        return retryOnConflict;
    }

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
    public static class Builder extends WriteDataObjectRequest.Builder<Builder> {
        private int retryOnConflict = 0;
        private ToXContentObject dataObject = null;

        /**
         * Retry the update request on a version conflict exception.
         * @param retries Number of times to retry, if positive, otherwise will not retry
         * @return the updated builder
         */
        public Builder retryOnConflict(int retries) {
            this.retryOnConflict = retries;
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
            this.dataObject = new ToXContentObject() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.map(dataObjectMap);
                }
            };
            return this;
        }

        /**
         * Builds the request
         * @return A {@link UpdateDataObjectRequest}
         */
        public UpdateDataObjectRequest build() {
            WriteDataObjectRequest.validateSeqNoAndPrimaryTerm(this.ifSeqNo, this.ifPrimaryTerm, false);
            return new UpdateDataObjectRequest(
                this.index,
                this.id,
                this.tenantId,
                this.ifSeqNo,
                this.ifPrimaryTerm,
                this.retryOnConflict,
                this.dataObject
            );
        }
    }
}
