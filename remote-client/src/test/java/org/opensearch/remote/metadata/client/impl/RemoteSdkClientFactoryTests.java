/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import org.opensearch.OpenSearchException;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.common.TestClassLoader;
import org.opensearch.transport.client.Client;
import org.junit.AfterClass;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_OPENSEARCH;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_AWARE_KEY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RemoteSdkClientFactoryTests {

    private static final List<AutoCloseable> resourcesToClose = new ArrayList<>();

    @AfterClass
    public static void closeResources() throws Exception {
        for (AutoCloseable resource : resourcesToClose) {
            resource.close();
        }
        resourcesToClose.clear();
    }

    @Test
    public void testRemoteOpenSearchBinding() {
        Map<String, String> settings = Map.ofEntries(
            Map.entry(REMOTE_METADATA_TYPE_KEY, REMOTE_OPENSEARCH),
            Map.entry(REMOTE_METADATA_ENDPOINT_KEY, "http://example.org"),
            Map.entry(REMOTE_METADATA_REGION_KEY, "eu-west-3")
        );
        SdkClient sdkClient = SdkClientFactory.createSdkClient(mock(Client.class), NamedXContentRegistry.EMPTY, settings);
        assertTrue(sdkClient.getDelegate() instanceof RemoteClusterIndicesClient);
        resourcesToClose.add(sdkClient.getDelegate());
    }

    @Test
    public void testRemoteOpenSearchBindingException() {
        Map<String, String> settings = Map.of(REMOTE_METADATA_TYPE_KEY, REMOTE_OPENSEARCH);
        assertThrows(
            OpenSearchException.class,
            () -> SdkClientFactory.createSdkClient(mock(Client.class), NamedXContentRegistry.EMPTY, settings)
        );
    }

    @Test
    public void testCreateSdkClient_RemoteCluster() {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, REMOTE_OPENSEARCH);
        metadataSettings.put(TENANT_AWARE_KEY, "true");
        metadataSettings.put(REMOTE_METADATA_ENDPOINT_KEY, "http://example.org");

        Thread.currentThread().setContextClassLoader(new TestClassLoader());

        SdkClient client = SdkClientFactory.createSdkClient(mock(Client.class), NamedXContentRegistry.EMPTY, metadataSettings);

        assertNotNull(client);
        assertTrue(client.getDelegate() instanceof RemoteClusterIndicesClient);
        resourcesToClose.add(client.getDelegate());
    }

    @Test
    public void testCreateSdkClient_NoSuitableImplementation() {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, "UNSUPPORTED_TYPE");
        metadataSettings.put(TENANT_AWARE_KEY, "true");
        metadataSettings.put(REMOTE_METADATA_ENDPOINT_KEY, "http://example.org");

        Thread.currentThread().setContextClassLoader(new TestClassLoader());

        SdkClient client = SdkClientFactory.createSdkClient(mock(Client.class), NamedXContentRegistry.EMPTY, metadataSettings);

        assertNotNull(client);
        assertTrue(client.getDelegate() instanceof LocalClusterIndicesClient);
    }
}
