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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BulkDataObjectRequestTests {
    private String testIndex;
    private String testGlobalIndex;
    private String testTenantId;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testGlobalIndex = "test-global-index";
        testTenantId = "test-tenant-id";
    }

    @Test
    public void testBulkDataObjectRequest() {
        BulkDataObjectRequest request = BulkDataObjectRequest.builder()
            .globalIndex(testGlobalIndex)
            .build()
            .add(PutDataObjectRequest.builder().index(testIndex).build())
            .add(UpdateDataObjectRequest.builder().build())
            .add(DeleteDataObjectRequest.builder().index(testIndex).tenantId(testTenantId).build())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        assertEquals(Set.of(testIndex, testGlobalIndex), request.getIndices());
        assertEquals(3, request.requests().size());
        assertEquals(RefreshPolicy.IMMEDIATE, request.getRefreshPolicy());

        DataObjectRequest r0 = request.requests().get(0);
        assertTrue(r0 instanceof PutDataObjectRequest);
        assertEquals(testIndex, r0.index());
        assertNull(r0.tenantId());
        assertEquals(RefreshPolicy.NONE, ((WriteDataObjectRequest) r0).getRefreshPolicy());

        DataObjectRequest r1 = request.requests().get(1);
        assertTrue(r1 instanceof UpdateDataObjectRequest);
        assertEquals(testGlobalIndex, r1.index());
        assertNull(r1.tenantId());
        assertEquals(RefreshPolicy.NONE, ((WriteDataObjectRequest) r1).getRefreshPolicy());

        DataObjectRequest r2 = request.requests().get(2);
        assertTrue(r2 instanceof DeleteDataObjectRequest);
        assertEquals(testIndex, r2.index());
        assertEquals(testTenantId, r2.tenantId());
        assertEquals(RefreshPolicy.NONE, ((WriteDataObjectRequest) r2).getRefreshPolicy());
    }

    @Test
    public void testBulkDataObjectRequest_Tenant() {
        BulkDataObjectRequest request = BulkDataObjectRequest.builder()
            .build()
            .add(PutDataObjectRequest.builder().index(testIndex).tenantId(testTenantId).build())
            .add(DeleteDataObjectRequest.builder().index(testIndex).tenantId(testTenantId).build());

        assertEquals(Set.of(testIndex), request.getIndices());
        assertEquals(2, request.requests().size());

        DataObjectRequest r0 = request.requests().get(0);
        assertTrue(r0 instanceof PutDataObjectRequest);
        assertEquals(testIndex, r0.index());
        assertEquals(testTenantId, r0.tenantId());
        assertEquals(RefreshPolicy.NONE, ((WriteDataObjectRequest) r0).getRefreshPolicy());

        DataObjectRequest r1 = request.requests().get(1);
        assertTrue(r1 instanceof DeleteDataObjectRequest);
        assertEquals(testIndex, r1.index());
        assertEquals(testTenantId, r1.tenantId());
        assertEquals(RefreshPolicy.NONE, ((WriteDataObjectRequest) r1).getRefreshPolicy());
    }

    @SuppressWarnings("removal")
    @Test
    public void testBulkDataObjectRequest_Exceptions() {
        PutDataObjectRequest nullIndexRequest = PutDataObjectRequest.builder().build();
        // Remove in 4.0.0 when add(DataObjectRequest) method is removed
        GetDataObjectRequest badTypeRequest = GetDataObjectRequest.builder().index(testIndex).build();

        BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder().build();
        assertThrows(IllegalArgumentException.class, () -> bulkRequest.add(nullIndexRequest));
        // Remove in 4.0.0 when add(DataObjectRequest) method is removed
        assertThrows(IllegalArgumentException.class, () -> bulkRequest.add(badTypeRequest));
    }
}
