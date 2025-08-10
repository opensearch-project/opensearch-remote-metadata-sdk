/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * An abstract class for write operations that support sequence numbers and primary terms
 */
public abstract class WriteDataObjectRequest extends DataObjectRequest {
    private final Long ifSeqNo;
    private final Long ifPrimaryTerm;

    protected WriteDataObjectRequest(String index, String id, String tenantId, Long ifSeqNo, Long ifPrimaryTerm) {
        super(index, id, tenantId);
        this.ifSeqNo = ifSeqNo;
        this.ifPrimaryTerm = ifPrimaryTerm;
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
    }
}
