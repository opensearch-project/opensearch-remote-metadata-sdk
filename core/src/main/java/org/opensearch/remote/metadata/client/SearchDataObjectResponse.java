/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;

import java.io.IOException;

/**
 * A class abstracting an OpenSearch SearchResponse
 */
public class SearchDataObjectResponse {
    // Only one of these will be non-null
    private final XContentParser parser;
    private final SearchResponse searchResponse;

    /**
     * Instantiate this response with a {@link SearchResponse}.
     * @param searchResponse a pre-completed Search response
     */
    public SearchDataObjectResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
        this.parser = null;
    }

    /**
     * Instantiate this response with a parser used to recreate a {@link SearchResponse}.
     * @param parser an XContentParser that can be used to create the response.
     */
    public SearchDataObjectResponse(XContentParser parser) {
        this.parser = parser;
        this.searchResponse = null;
    }

    /**
     * Returns the SearchResponse object
     * @return the search response if present, or parsed otherwise
     */
    public @Nullable SearchResponse searchResponse() {
        if (this.searchResponse == null) {
            try {
                return SearchResponse.fromXContent(this.parser);
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return this.searchResponse;
    }

    /**
     * Returns the parser
     * @return the parser
     * @throws IOException on a failure to create the parser if created from the SearchResponse object
     */
    public XContentParser parser() {
        if (this.parser == null) {
            try {
                return SdkClientUtils.createParser(searchResponse);
            } catch (IOException | NullPointerException e) {
                return null;
            }
        }
        return this.parser;
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
    public static class Builder {
        private XContentParser parser = null;

        /**
         * Empty Constructor for the Builder object
         */
        private Builder() {}

        /**
         * Add a parser to this builder
         * @param parser a parser
         * @return the updated builder
         */
        public Builder parser(XContentParser parser) {
            this.parser = parser;
            return this;
        }

        /**
         * Builds the response
         * @return A {@link SearchDataObjectResponse}
         */
        public SearchDataObjectResponse build() {
            return new SearchDataObjectResponse(this.parser);
        }
    }
}
