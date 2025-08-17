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
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A class abstracting an OpenSearch BulkRequest
 */
public class BulkDataObjectRequest {

    private final List<DataObjectRequest> requests = new ArrayList<>();
    private final Set<String> indices = new HashSet<>();
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;
    private TimeValue timeout = TimeValue.timeValueMinutes(1L);
    private String globalIndex;

    /**
     * Instantiate this request with a global index.
     * <p>
     * For data storage implementations other than OpenSearch, an index may be referred to as a table and the id may be referred to as a primary key.
     * @param globalIndex the index location for all the bulk requests as a default if not already specified
     */
    public BulkDataObjectRequest(@Nullable String globalIndex) {
        this.globalIndex = globalIndex;
    }

    /**
     * Returns the list of requests in this bulk request.
     * @return the requests list
     */
    public List<DataObjectRequest> requests() {
        return List.copyOf(this.requests);
    }

    /**
     * Returns the indices being updated in this bulk request.
     * @return the indices being updated
     */
    public Set<String> getIndices() {
        return Collections.unmodifiableSet(indices);
    }

    /**
     * Add the given request to the {@link BulkDataObjectRequest}
     * @param request The request to add (will always result in an exception)
     * @return never returns as this method always throws an exception
     * @throws IllegalArgumentException always thrown, as request is not a WriteDataObjectRequest
     * @deprecated This method is deprecated in favor of {@link #add(WriteDataObjectRequest)}.
     * Due to Java's method overload resolution rules (JLS §15.12.2), any object that is an instance of WriteDataObjectRequest
     * will automatically be routed to the more specific add(WriteDataObjectRequest) method rather than this method.
     * The method is retained only for SemVer compile-time compatibility.
     */
    @Deprecated(since = "3.3.0", forRemoval = true)
    public BulkDataObjectRequest add(DataObjectRequest request) {
        throw new IllegalArgumentException("No support for request [" + request.getClass().getName() + "]");
    }

    /**
     * Add the given request to the {@link BulkDataObjectRequest}
     * @param <R> The specific type of WriteDataObjectRequest
     * @param request The request to add
     * @return the updated request object
     */
    public <R extends WriteDataObjectRequest<R>> BulkDataObjectRequest add(R request) {
        if (Strings.isNullOrEmpty(request.index())) {
            if (Strings.isNullOrEmpty(globalIndex)) {
                throw new IllegalArgumentException(
                    "Either the request [" + request.getClass().getName() + "] or the bulk request must specify an index."
                );
            }
            indices.add(globalIndex);
            request.index(globalIndex);
        } else {
            indices.add(request.index());
        }
        // Bulk Requests require this on individual requests
        request.setRefreshPolicy(RefreshPolicy.NONE);
        requests.add(request);
        return this;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}, the default).
     * Note this applies to the combined Bulk Request itself, the individual requests all use {@linkplain RefreshPolicy#NONE}.
     * May not be applicable on all clients. Defaults to {@code IMMEDIATE}.
     * @param refreshPolicy the refresh policy
     * @return the updated request
     */
    public BulkDataObjectRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}, the default).
     * Note this applies to the combined Bulk Request itself, the individual requests all use {@linkplain RefreshPolicy#NONE}.
     * May not be applicable on all clients. Defaults to {@code IMMEDIATE}.
     * @return the refresh policy
     */
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. May not be applicable on all clients. Defaults to {@code 1m}.
     * @param timeout The timeout to set
     * @return the request after updating the timeout
     */
    public final BulkDataObjectRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. May not be applicable on all clients. Defaults to {@code 1m}.
     * @param timeout The timeout to set
     * @return the request after updating the timeout
     */
    public final BulkDataObjectRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. May not be applicable on all clients. Defaults to {@code 1m}.
     * @return the timeout
     */
    public TimeValue timeout() {
        return timeout;
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
        private String globalIndex = null;

        /**
         * Empty constructor to initialize
         */
        protected Builder() {}

        /**
         * Add an index to this builder
         * @param index the index to put the object
         * @return the updated builder
         */
        public Builder globalIndex(String index) {
            this.globalIndex = index;
            return this;
        }

        /**
         * Builds the request
         * @return A {@link BulkDataObjectRequest}
         */
        public BulkDataObjectRequest build() {
            return new BulkDataObjectRequest(this.globalIndex);
        }
    }
}
