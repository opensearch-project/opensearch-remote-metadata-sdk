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
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.opensearch.action.bulk.BulkResponse.NO_INGEST_TOOK;

/**
 * A class abstracting an OpenSearch BulkResponse
 */
public class BulkDataObjectResponse {

    private final DataObjectResponse[] responses;
    private final long tookInMillis;
    private final long ingestTookInMillis;
    private final boolean failures;
    // Only one of these will be non-null
    private final XContentParser parser;
    private final BulkResponse bulkResponse;

    /**
     * Instantiate this response with a {@link BulkResponse}.
     * @param bulkResponse a pre-completed Bulk response
     */
    public BulkDataObjectResponse(BulkResponse bulkResponse) {
        this(
            generateDataObjectResponseArray(bulkResponse),
            bulkResponse.getTook().millis(),
            NO_INGEST_TOOK,
            bulkResponse.hasFailures(),
            null,
            bulkResponse
        );
    }

    /**
     * Generate an array of DataObjectResponses from a BulkResponse
     * @param bulkResponse The BulkResponse
     * @return An array of DataObjectResponse corresponding to the items. Array elements may be null on failed responses.
     */
    private static DataObjectResponse[] generateDataObjectResponseArray(BulkResponse bulkResponse) {
        return Arrays.stream(bulkResponse.getItems()).map(itemResponse -> switch (itemResponse.getOpType()) {
            case INDEX, CREATE -> new PutDataObjectResponse(itemResponse);
            case UPDATE -> new UpdateDataObjectResponse(itemResponse);
            case DELETE -> new DeleteDataObjectResponse(itemResponse);
            default -> throw new OpenSearchException("Invalid operation type for bulk response", RestStatus.INTERNAL_SERVER_ERROR);
        }).toArray(DataObjectResponse[]::new);
    }

    /**
     * Instantiate this response
     * @param responses an array of responses
     * @param tookInMillis the time taken to process, in milliseconds
     * @param failures whether there are any failures in the responses
     * @param parser a parser that can be used to recreate the object
     */
    public BulkDataObjectResponse(DataObjectResponse[] responses, long tookInMillis, boolean failures, XContentParser parser) {
        this(responses, tookInMillis, NO_INGEST_TOOK, failures, parser, null);
    }

    /**
     * Instantiate this response
     * @param responses an array of responses
     * @param tookInMillis the time taken to process, in milliseconds
     * @param ingestTookInMillis the time taken to process ingest, in milliseconds
     * @param failures whether there are any failures in the responses
     * @param parser a parser that can be used to recreate the object
     */
    public BulkDataObjectResponse(
        DataObjectResponse[] responses,
        long tookInMillis,
        long ingestTookInMillis,
        boolean failures,
        XContentParser parser
    ) {
        this(responses, tookInMillis, ingestTookInMillis, failures, parser, null);
    }

    private BulkDataObjectResponse(
        DataObjectResponse[] responses,
        long tookInMillis,
        long ingestTookInMillis,
        boolean failures,
        XContentParser parser,
        BulkResponse bulkResponse
    ) {
        this.responses = responses;
        this.tookInMillis = tookInMillis;
        this.ingestTookInMillis = ingestTookInMillis;
        this.failures = failures;
        this.parser = parser;
        this.bulkResponse = bulkResponse;
    }

    /**
     * The items representing each action performed in the bulk operation (in the same order!).
     * @return the responses in the same order requested
     */
    public DataObjectResponse[] getResponses() {
        return responses;
    }

    /**
     * How long the bulk execution took. Excluding ingest preprocessing.
     * @return the execution time in milliseconds
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * If ingest is enabled returns the bulk ingest preprocessing time. in milliseconds, otherwise -1 is returned.
     * @return the ingest execution time in milliseconds
     */
    public long getIngestTookInMillis() {
        return ingestTookInMillis;
    }

    /**
     * Has anything failed with the execution.
     * @return true if any response failed, false otherwise
     */
    public boolean hasFailures() {
        return this.failures;
    }

    /**
     * Returns the BulkhResponse object
     * @return the bulk response if present, or parsed otherwise
     */
    public @Nullable BulkResponse bulkResponse() {
        if (this.bulkResponse == null) {
            try {
                return BulkResponse.fromXContent(parser);
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return this.bulkResponse;
    }

    /**
     * Returns the parser
     * @return the parser
     */
    public XContentParser parser() {
        if (this.parser == null) {
            try {
                return SdkClientUtils.createParser(bulkResponse);
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return this.parser;
    }
}
