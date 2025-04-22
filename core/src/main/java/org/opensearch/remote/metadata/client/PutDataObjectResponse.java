/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;

import java.io.IOException;

/**
 * A class abstracting an OpenSearch IndexeResponse
 */
public class PutDataObjectResponse extends DataObjectResponse {

    // If populated directly, will populate superclass fields
    private final IndexResponse indexResponse;

    /**
     * Instantiate this response with an {@link IndexResponse}.
     * @param indexResponse a pre-completed Index response
     */
    public PutDataObjectResponse(IndexResponse indexResponse) {
        super(indexResponse.getIndex(), indexResponse.getId(), null, false, null, null);
        this.indexResponse = indexResponse;
    }

    /**
     * Instantiate this response with a {@link BulkItemResponse} for an index operation.
     * @param itemResponse a bulk item response for an index operation
     */
    public PutDataObjectResponse(BulkItemResponse itemResponse) {
        super(
            itemResponse.getIndex(),
            itemResponse.getId(),
            null,
            itemResponse.isFailed(),
            itemResponse.getFailure() != null ? itemResponse.getFailure().getCause() : null,
            itemResponse.getFailure() != null ? itemResponse.getFailure().getStatus() : null
        );
        DocWriteResponse response = itemResponse.getResponse();
        if (response == null) {
            this.indexResponse = null;
        } else if (response instanceof IndexResponse) {
            this.indexResponse = (IndexResponse) response;
        } else {
            throw new OpenSearchException(
                "Expected IndexResponse but got " + response.getClass().getSimpleName(),
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }
    }

    /**
     * Instantiate this request with an id and parser representing an IndexResponse
     * <p>
     * For data storage implementations other than OpenSearch, the id may be referred to as a primary key.
     * @param index the index
     * @param id the document id
     * @param parser a parser that can be used to create an IndexResponse
     * @param failed whether the request failed
     * @param cause the Exception causing the failure
     * @param status the RestStatus
     */
    public PutDataObjectResponse(String index, String id, XContentParser parser, boolean failed, Exception cause, RestStatus status) {
        super(index, id, parser, failed, cause, status);
        this.indexResponse = null;
    }

    /**
     * Returns the IndexResponse object
     * @return the index response if present, or parsed otherwise
     */
    public @Nullable IndexResponse indexResponse() {
        if (this.indexResponse == null) {
            try {
                return IndexResponse.fromXContent(parser());
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return this.indexResponse;
    }

    @Override
    public XContentParser parser() {
        if (super.parser() == null) {
            try {
                return SdkClientUtils.createParser(this.indexResponse);
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return super.parser();
    }

    /**
     * Instantiate a builder for this object
     * @return a builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Class for constructing a Builder for this Response Object
     */
    public static class Builder extends DataObjectResponse.Builder<Builder> {

        /**
         * Builds the response
         * @return A {@link PutDataObjectResponse}
         */
        public PutDataObjectResponse build() {
            return new PutDataObjectResponse(this.index, this.id, this.parser, this.failed, this.cause, this.status);
        }
    }
}
