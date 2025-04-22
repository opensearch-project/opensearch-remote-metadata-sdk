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
import org.opensearch.action.search.SearchResponse.Clusters;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.search.internal.InternalSearchResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class SearchDataObjectResponseTests {

    private XContentParser testParser;
    private SearchResponse testSearchResponse;

    @BeforeEach
    public void setUp() {
        testParser = mock(XContentParser.class);
        testSearchResponse = new SearchResponse(
            InternalSearchResponse.empty(),
            null,
            1,
            1,
            0,
            100L,
            ShardSearchFailure.EMPTY_ARRAY,
            Clusters.EMPTY
        );
    }

    @Test
    public void testSearchDataObjectResponse_FromParser() throws IOException {
        SearchDataObjectResponse response = SearchDataObjectResponse.builder().parser(testParser).build();

        assertEquals(testParser, response.parser());
    }

    @Test
    public void testSearchDataObjectResponse_FromSearchResponse() throws IOException {
        SearchDataObjectResponse response = new SearchDataObjectResponse(testSearchResponse);
        assertSame(testSearchResponse, response.searchResponse());
        assertNotNull(response.parser());
    }

    @Test
    public void testSearchDataObjectResponse_SearchResponseFromParser() throws IOException {
        String searchResponseJson = "{\n"
            + "  \"took\" : 1,\n"
            + "  \"timed_out\" : false,\n"
            + "  \"_shards\" : {\n"
            + "    \"total\" : 1,\n"
            + "    \"successful\" : 1,\n"
            + "    \"skipped\" : 0,\n"
            + "    \"failed\" : 0\n"
            + "  },\n"
            + "  \"hits\" : {\n"
            + "    \"total\" : {\n"
            + "      \"value\" : 0,\n"
            + "      \"relation\" : \"eq\"\n"
            + "    },\n"
            + "    \"max_score\" : null,\n"
            + "    \"hits\" : [ ]\n"
            + "  }\n"
            + "}";

        XContentParser parser = SdkClientUtils.createParser(searchResponseJson);
        SearchDataObjectResponse response = new SearchDataObjectResponse(parser);
        SearchResponse parsedResponse = response.searchResponse();
        assertNotNull(parsedResponse);
        assertEquals(1L, parsedResponse.getTook().millis());
        assertFalse(parsedResponse.isTimedOut());
    }

    @Test
    public void testBuilder_Empty() {
        // Builder should work with no parameters set
        SearchDataObjectResponse response = SearchDataObjectResponse.builder().build();
        assertNotNull(response);
    }
}
