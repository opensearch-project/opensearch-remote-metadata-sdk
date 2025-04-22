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
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;

import java.io.IOException;

/**
 * A class abstracting an OpenSearch UpdateResponse
 */
public class UpdateDataObjectResponse extends DataObjectResponse {

    // If populated directly, will populate superclass fields
    private final UpdateResponse updateResponse;

    /**
     * Instantiate this response with an {@link UpdateResponse}.
     * @param updateResponse a pre-completed Update response
     */
    public UpdateDataObjectResponse(UpdateResponse updateResponse) {
        super(updateResponse.getIndex(), updateResponse.getId(), null, false, null, null);
        this.updateResponse = updateResponse;
    }

    /**
     * Instantiate this response with a {@link BulkItemResponse} for an update operation.
     * @param itemResponse a bulk item response for an update operation
     */
    public UpdateDataObjectResponse(BulkItemResponse itemResponse) {
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
            this.updateResponse = null;
        } else if (response instanceof UpdateResponse) {
            this.updateResponse = (UpdateResponse) response;
        } else {
            throw new OpenSearchException(
                "Expected UpdateResponse but got " + response.getClass().getSimpleName(),
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }
    }

    /**
     * Instantiate this request with an id and parser representing an UpdateResponse
     * <p>
     * For data storage implementations other than OpenSearch, the id may be referred to as a primary key.
     * @param index the index
     * @param id the document id
     * @param parser a parser that can be used to create an UpdateResponse
     * @param failed whether the request failed
     * @param cause the Exception causing the failure
     * @param status the RestStatus
     */
    public UpdateDataObjectResponse(String index, String id, XContentParser parser, boolean failed, Exception cause, RestStatus status) {
        super(index, id, parser, failed, cause, status);
        this.updateResponse = null;
    }

    /**
     * Returns the UpdateResponse object
     * @return the update response if present, or parsed otherwise
     */
    public @Nullable UpdateResponse updateResponse() {
        if (this.updateResponse == null) {
            try {
                return UpdateResponse.fromXContent(parser());
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return this.updateResponse;
    }

    @Override
    public XContentParser parser() {
        if (super.parser() == null) {
            try {
                return SdkClientUtils.createParser(this.updateResponse);
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
         * @return A {@link UpdateDataObjectResponse}
         */
        public UpdateDataObjectResponse build() {
            return new UpdateDataObjectResponse(this.index, this.id, this.parser, this.failed, this.cause, this.status);
        }
    }
}
