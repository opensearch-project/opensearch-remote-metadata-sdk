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

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * An abstract class for write operations that support sequence numbers and primary terms
 */
public abstract class WriteDataObjectRequest extends DataObjectRequest {
    protected final Long ifSeqNo;
    protected final Long ifPrimaryTerm;
    protected RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    /**
     * Instantiate this request with an index, id, and concurrency information.
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table and the id may be referred to as a primary key.
     * @param index the index location to delete the object
     * @param id the document id
     * @param tenantId the tenant id
     * @param ifSeqNo the sequence number to match or null if not required
     * @param ifPrimaryTerm the primary term to match or null if not required
     * @param refreshPolicy when should the written data be refreshed. May not be applicable on all clients.
     * @param isCreateOperation whether this can only create a new document and not overwrite one
     */
    protected WriteDataObjectRequest(
        String index,
        String id,
        String tenantId,
        Long ifSeqNo,
        Long ifPrimaryTerm,
        RefreshPolicy refreshPolicy,
        boolean isCreateOperation
    ) {
        super(index, id, tenantId);
        validateSeqNoAndPrimaryTerm(ifSeqNo, ifPrimaryTerm, isCreateOperation);
        this.ifSeqNo = ifSeqNo;
        this.ifPrimaryTerm = ifPrimaryTerm;
        if (refreshPolicy != null) {
            this.refreshPolicy = refreshPolicy;
        }
    }

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
     * Returns the refresh policy.
     * @return the refresh policy.
     */
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * Sets the refresh policy.
     * @param refreshPolicy The policy to set
     */
    public void setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }

    @Override
    public boolean isWriteRequest() {
        return true;
    }

    /**
     * Builder for write requests that support sequence numbers and primary terms
     */
    public static abstract class Builder<T extends Builder<T>> extends DataObjectRequest.Builder<T> {
        protected Long ifSeqNo = null;
        protected Long ifPrimaryTerm = null;
        protected RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

        /**
         * Only perform this request if the document's modification was assigned the given
         * sequence number. Must be used in combination with {@link #ifPrimaryTerm(long)}
         * @param seqNo the sequence number
         * @return the updated builder
         */
        public T ifSeqNo(long seqNo) {
            if (seqNo < 0 && seqNo != UNASSIGNED_SEQ_NO) {
                throw new IllegalArgumentException("sequence numbers must be non negative. got [" + seqNo + "].");
            }
            this.ifSeqNo = seqNo;
            return self();
        }

        /**
         * Only performs this request if the document's last modification was assigned the given
         * primary term. Must be used in combination with {@link #ifSeqNo(long)}
         * @param term the primary term
         * @return the updated builder
         */
        public T ifPrimaryTerm(long term) {
            if (term < 0) {
                throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
            }
            this.ifPrimaryTerm = term;
            return self();
        }

        public T refreshPolicy(RefreshPolicy refreshPolicy) {
            this.refreshPolicy = refreshPolicy;
            return self();
        }
    }

    /**
     * Validates sequence number and primary term including a check on create optype
     * @param ifSeqNo the sequence number
     * @param ifPrimaryTerm the primary term
     * @param createOperation whether this is a create operation that does not support seqNo/primaryTerm
     * @throws IllegalArgumentException if validation fails
     */
    protected static void validateSeqNoAndPrimaryTerm(Long ifSeqNo, Long ifPrimaryTerm, boolean createOperation) {
        if (createOperation && !(isUnassignedSeqNo(ifSeqNo) && isUnassignedPrimaryTerm(ifPrimaryTerm))) {
            throw new IllegalArgumentException(
                "create operations (overwriteIfExists=false) do not support compare and set with seqNo and primaryTerm."
            );
        }
        if (isUnassignedSeqNo(ifSeqNo) != isUnassignedPrimaryTerm(ifPrimaryTerm)) {
            throw new IllegalArgumentException("Both ifSeqNo and ifPrimaryTerm must be set or unassigned.");
        }
    }

    private static boolean isUnassignedSeqNo(Long seqNo) {
        return seqNo == null || seqNo == UNASSIGNED_SEQ_NO;
    }

    private static boolean isUnassignedPrimaryTerm(Long primaryTerm) {
        return primaryTerm == null || primaryTerm == UNASSIGNED_PRIMARY_TERM;
    }

}
