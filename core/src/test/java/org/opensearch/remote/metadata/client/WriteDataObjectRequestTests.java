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
import org.junit.jupiter.api.Test;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.remote.metadata.client.WriteDataObjectRequest.validateSeqNoAndPrimaryTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WriteDataObjectRequestTests {

    private static final String TEST_INDEX = "test-index";
    private static final String TEST_ID = "test-id";
    private static final String TEST_TENANT_ID = "test-tenant";

    // Concrete implementation for testing
    private static class TestWriteRequest extends WriteDataObjectRequest<TestWriteRequest> {
        TestWriteRequest(String index, String id, String tenantId, Long ifSeqNo, Long ifPrimaryTerm, RefreshPolicy refreshPolicy) {
            super(index, id, tenantId, ifSeqNo, ifPrimaryTerm, refreshPolicy, false);
        }

        public static class Builder extends WriteDataObjectRequest.Builder<Builder> {
            public TestWriteRequest build() {
                validateSeqNoAndPrimaryTerm(this.ifSeqNo, this.ifPrimaryTerm, false);
                return new TestWriteRequest(index, id, tenantId, ifSeqNo, ifPrimaryTerm, refreshPolicy);
            }
        }

        public static Builder builder() {
            return new Builder();
        }
    }

    @Test
    public void testConstructorValidation() {
        // Valid cases
        TestWriteRequest request = new TestWriteRequest(TEST_INDEX, TEST_ID, TEST_TENANT_ID, null, null, null);
        assertNull(request.ifSeqNo());
        assertNull(request.ifPrimaryTerm());

        request = new TestWriteRequest(TEST_INDEX, TEST_ID, TEST_TENANT_ID, 1L, 1L, null);
        assertEquals(1L, request.ifSeqNo());
        assertEquals(1L, request.ifPrimaryTerm());

        // Invalid cases
        assertThrows(
            IllegalArgumentException.class,
            () -> new TestWriteRequest(TEST_INDEX, TEST_ID, TEST_TENANT_ID, 1L, null, null),
            "Should throw when only seqNo is provided"
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new TestWriteRequest(TEST_INDEX, TEST_ID, TEST_TENANT_ID, null, 1L, null),
            "Should throw when only primaryTerm is provided"
        );
    }

    @Test
    public void testBuilderValidation() {
        // Valid cases
        TestWriteRequest request = TestWriteRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();
        assertNull(request.ifSeqNo());
        assertNull(request.ifPrimaryTerm());

        request = TestWriteRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).ifSeqNo(1L).ifPrimaryTerm(1L).build();
        assertEquals(1L, request.ifSeqNo());
        assertEquals(1L, request.ifPrimaryTerm());

        // Invalid cases
        TestWriteRequest.Builder builder = TestWriteRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).ifSeqNo(1L);
        assertThrows(IllegalArgumentException.class, builder::build, "Should throw when only seqNo is provided");

        builder = TestWriteRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).ifPrimaryTerm(1L);
        assertThrows(IllegalArgumentException.class, builder::build, "Should throw when only primaryTerm is provided");
    }

    @Test
    public void testNegativeValues() {
        TestWriteRequest.Builder builder = TestWriteRequest.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.ifSeqNo(-1L), "Should throw on negative sequence number");

        assertThrows(IllegalArgumentException.class, () -> builder.ifPrimaryTerm(-1L), "Should throw on negative primary term");
    }

    @Test
    public void testCreateOperationValidation() {
        assertThrows(
            IllegalArgumentException.class,
            () -> validateSeqNoAndPrimaryTerm(1L, 1L, true),
            "Should throw when using seqNo/primaryTerm with create operation"
        );

        // Should not throw
        validateSeqNoAndPrimaryTerm(null, null, true);
        validateSeqNoAndPrimaryTerm(1L, 1L, false);
    }

    @Test
    public void testIsWriteRequest() {
        TestWriteRequest request = new TestWriteRequest(TEST_INDEX, TEST_ID, TEST_TENANT_ID, null, null, null);
        assertTrue(request.isWriteRequest());
    }

    @Test
    public void testUnassignedValues() {
        // Test combinations of null and UNASSIGNED values
        validateSeqNoAndPrimaryTerm(UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, false);
        validateSeqNoAndPrimaryTerm(null, UNASSIGNED_PRIMARY_TERM, false);
        validateSeqNoAndPrimaryTerm(UNASSIGNED_SEQ_NO, null, false);

        // Should fail when mixing unassigned and assigned values
        assertThrows(IllegalArgumentException.class, () -> validateSeqNoAndPrimaryTerm(UNASSIGNED_SEQ_NO, 1L, false));
        assertThrows(IllegalArgumentException.class, () -> validateSeqNoAndPrimaryTerm(1L, UNASSIGNED_PRIMARY_TERM, false));
    }

    @Test
    public void testRefreshPolicyDefault() {
        TestWriteRequest request = new TestWriteRequest(TEST_INDEX, TEST_ID, TEST_TENANT_ID, null, null, null);
        assertEquals(RefreshPolicy.IMMEDIATE, request.getRefreshPolicy());
    }

    @Test
    public void testRefreshPolicyBuilderDefault() {
        TestWriteRequest request = TestWriteRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();
        assertEquals(RefreshPolicy.IMMEDIATE, request.getRefreshPolicy());

        request = TestWriteRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .refreshPolicy(RefreshPolicy.NONE)
            .build();
        assertEquals(RefreshPolicy.NONE, request.getRefreshPolicy());
    }

    @Test
    public void testRefreshPolicyExplicit() {
        TestWriteRequest request = new TestWriteRequest(TEST_INDEX, TEST_ID, TEST_TENANT_ID, null, null, RefreshPolicy.NONE);
        assertEquals(RefreshPolicy.NONE, request.getRefreshPolicy());

        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        assertEquals(RefreshPolicy.WAIT_UNTIL, request.getRefreshPolicy());

        assertEquals(RefreshPolicy.IMMEDIATE, request.setRefreshPolicy(RefreshPolicy.IMMEDIATE).getRefreshPolicy());
    }
}
