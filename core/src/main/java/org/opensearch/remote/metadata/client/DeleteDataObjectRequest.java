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

/**
 * A class abstracting an OpenSearch DeleteRequest
 */
public class DeleteDataObjectRequest extends WriteDataObjectRequest<DeleteDataObjectRequest> {

    /**
     * Instantiate this request with an index and id.
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table and the id may be referred to as a primary key.
     * @param index the index location to delete the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param ifSeqNo the sequence number to match or null if not required
     * @param ifPrimaryTerm the primary term to match or null if not required
     * @param refreshPolicy when should the written data be refreshed. May not be applicable on all clients. Defaults to {@code IMMEDIATE}.
     * @param timeout A timeout to wait if the index operation can't be performed immediately. May not be applicable on all clients. Defaults to {@code 1m}.
     */
    public DeleteDataObjectRequest(
        String index,
        String id,
        String tenantId,
        Long ifSeqNo,
        Long ifPrimaryTerm,
        RefreshPolicy refreshPolicy,
        TimeValue timeout
    ) {
        super(index, id, tenantId, ifSeqNo, ifPrimaryTerm, refreshPolicy, timeout, false, null);
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

        /**
         * Builds the object
         * @return A {@link DeleteDataObjectRequest}
         */
        public DeleteDataObjectRequest build() {
            WriteDataObjectRequest.validateSeqNoAndPrimaryTerm(this.ifSeqNo, this.ifPrimaryTerm, false);
            return new DeleteDataObjectRequest(
                this.index,
                this.id,
                this.tenantId,
                this.ifSeqNo,
                this.ifPrimaryTerm,
                this.refreshPolicy,
                this.timeout
            );
        }
    }
}
