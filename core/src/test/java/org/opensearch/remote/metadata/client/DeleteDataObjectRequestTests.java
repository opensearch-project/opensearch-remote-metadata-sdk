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
import org.opensearch.common.unit.TimeValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeleteDataObjectRequestTests {
    private String testIndex;
    private String testId;
    private String testTenantId;
    private Long testSeqNo;
    private Long testPrimaryTerm;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testTenantId = "test-tenant-id";
        testSeqNo = 42L;
        testPrimaryTerm = 6L;
    }

    @Test
    public void testDeleteDataObjectRequest() {
        DeleteDataObjectRequest request = DeleteDataObjectRequest.builder().index(testIndex).id(testId).tenantId(testTenantId).build();

        assertEquals(testIndex, request.index());
        assertEquals(testId, request.id());
        assertEquals(testTenantId, request.tenantId());
        assertNull(request.ifSeqNo());
        assertNull(request.ifPrimaryTerm());
        assertEquals(RefreshPolicy.IMMEDIATE, request.getRefreshPolicy());
        assertEquals(TimeValue.timeValueMinutes(1L), request.timeout());
    }

    @Test
    public void testDeleteDataObjectRequestConcurrency() {
        DeleteDataObjectRequest request = DeleteDataObjectRequest.builder()
            .index(testIndex)
            .id(testId)
            .tenantId(testTenantId)
            .ifSeqNo(testSeqNo)
            .ifPrimaryTerm(testPrimaryTerm)
            .refreshPolicy(RefreshPolicy.NONE)
            .timeout("30s")
            .build();

        assertEquals(testIndex, request.index());
        assertEquals(testId, request.id());
        assertEquals(testTenantId, request.tenantId());
        assertEquals(testSeqNo, request.ifSeqNo());
        assertEquals(testPrimaryTerm, request.ifPrimaryTerm());
        assertEquals(RefreshPolicy.NONE, request.getRefreshPolicy());
        assertEquals(TimeValue.timeValueSeconds(30L), request.timeout());

        final DeleteDataObjectRequest.Builder badSeqNoBuilder = DeleteDataObjectRequest.builder();
        assertThrows(IllegalArgumentException.class, () -> badSeqNoBuilder.ifSeqNo(-99));
        final DeleteDataObjectRequest.Builder badPrimaryTermBuilder = DeleteDataObjectRequest.builder();
        assertThrows(IllegalArgumentException.class, () -> badPrimaryTermBuilder.ifPrimaryTerm(-99));
        final DeleteDataObjectRequest.Builder onlySeqNoBuilder = DeleteDataObjectRequest.builder().ifSeqNo(testSeqNo);
        assertThrows(IllegalArgumentException.class, () -> onlySeqNoBuilder.build());
        final DeleteDataObjectRequest.Builder onlyPrimaryTermBuilder = DeleteDataObjectRequest.builder().ifPrimaryTerm(testPrimaryTerm);
        assertThrows(IllegalArgumentException.class, () -> onlyPrimaryTermBuilder.build());
    }
}
