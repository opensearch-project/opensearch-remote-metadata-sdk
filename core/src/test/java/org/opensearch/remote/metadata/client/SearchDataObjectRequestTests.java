/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SearchDataObjectRequestTests {

    private String[] testIndices;
    private String testTenantId;
    private SearchSourceBuilder testSearchSourceBuilder;

    @BeforeEach
    public void setUp() {
        testIndices = new String[] { "test-index" };
        testTenantId = "test-tenant-id";
        testSearchSourceBuilder = new SearchSourceBuilder();
    }

    @Test
    public void testGetDataObjectRequest() {
        SearchDataObjectRequest request = SearchDataObjectRequest.builder()
            .indices(testIndices)
            .tenantId(testTenantId)
            .searchSourceBuilder(testSearchSourceBuilder)
            .build();

        assertArrayEquals(testIndices, request.indices());
        assertEquals(testTenantId, request.tenantId());
        assertEquals(testSearchSourceBuilder, request.searchSourceBuilder());
        assertTrue(request.searchRemoteReplica());
    }

    @Test
    public void testSearchRemoteReplicaDefaultTrue() {
        SearchDataObjectRequest request = SearchDataObjectRequest.builder()
            .indices(testIndices)
            .tenantId(testTenantId)
            .searchSourceBuilder(testSearchSourceBuilder)
            .build();

        assertTrue(request.searchRemoteReplica());
    }

    @Test
    public void testSearchRemoteReplicaFalse() {
        SearchDataObjectRequest request = SearchDataObjectRequest.builder()
            .indices(testIndices)
            .tenantId(testTenantId)
            .searchSourceBuilder(testSearchSourceBuilder)
            .searchRemoteReplica(false)
            .build();

        assertFalse(request.searchRemoteReplica());
    }

    @Test
    public void testConstructorDefaultSearchRemoteReplica() {
        SearchDataObjectRequest request = new SearchDataObjectRequest(testIndices, testTenantId, testSearchSourceBuilder);

        assertTrue(request.searchRemoteReplica());
    }

    @Test
    public void testConstructorExplicitSearchRemoteReplica() {
        SearchDataObjectRequest request = new SearchDataObjectRequest(testIndices, testTenantId, testSearchSourceBuilder, false);

        assertFalse(request.searchRemoteReplica());
    }
}
