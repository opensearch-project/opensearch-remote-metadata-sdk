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
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;

import java.io.IOException;

/**
 * A class abstracting an OpenSearch DeleteResponse
 */
public class DeleteDataObjectResponse extends DataObjectResponse {

    // If populated directly, will populate superclass fields
    private final DeleteResponse deleteResponse;

    /**
     * Instantiate this response with a {@link DeleteResponse}.
     * @param deleteResponse a pre-completed Get response
     */
    public DeleteDataObjectResponse(DeleteResponse deleteResponse) {
        super(deleteResponse.getIndex(), deleteResponse.getId(), null, false, null, null);
        this.deleteResponse = deleteResponse;
    }

    /**
     * Instantiate this response with a {@link BulkItemResponse} for a delete operation.
     * @param itemResponse a bulk item response for a delete operation
     */
    public DeleteDataObjectResponse(BulkItemResponse itemResponse) {
        super(
            itemResponse.getIndex(),
            itemResponse.getId(),
            null,
            itemResponse.isFailed(),
            itemResponse.getFailure() != null ? itemResponse.getFailure().getCause() : null,
            itemResponse.getFailure() != null ? itemResponse.getFailure().getStatus() : null
        );
        DocWriteResponse response = itemResponse.getResponse();
        this.deleteResponse = switch (response) {
            case null -> null;
            case DeleteResponse dr -> dr;
            default -> throw new OpenSearchException(
                "Expected DeleteResponse but got " + response.getClass().getSimpleName(),
                RestStatus.INTERNAL_SERVER_ERROR
            );
        };
    }

    /**
     * Instantiate this request with an id and parser representing a DeleteResponse
     * <p>
     * For data storage implementations other than OpenSearch, the id may be referred to as a primary key.
     * @param index the index
     * @param id the document id
     * @param parser a parser that can be used to create a DeleteResponse
     * @param failed whether the request failed
     * @param cause the Exception causing the failure
     * @param status the RestStatus
     */
    public DeleteDataObjectResponse(String index, String id, XContentParser parser, boolean failed, Exception cause, RestStatus status) {
        super(index, id, parser, failed, cause, status);
        this.deleteResponse = null;
    }

    /**
     * Returns the DeleteResponse object
     * @return the delete response if present, or parsed otherwise
     */
    public @Nullable DeleteResponse deleteResponse() {
        if (this.deleteResponse == null) {
            try {
                return DeleteResponse.fromXContent(parser());
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return this.deleteResponse;
    }

    @Override
    public XContentParser parser() {
        if (super.parser() == null) {
            try {
                return SdkClientUtils.createParser(this.deleteResponse);
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
         * @return A {@link DeleteDataObjectResponse}
         */
        public DeleteDataObjectResponse build() {
            return new DeleteDataObjectResponse(this.index, this.id, this.parser, this.failed, this.cause, this.status);
        }
    }
}
