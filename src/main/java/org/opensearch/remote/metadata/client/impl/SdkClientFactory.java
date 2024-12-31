/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SdkClientDelegate;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_AWARE_KEY;

/**
 * A class to create a {@link SdkClient} with implementation based on settings
 */
public class SdkClientFactory {
    private static final Logger log = LogManager.getLogger(SdkClientFactory.class);

    /**
     * Create a new SdkClient with implementation determined by the value of the Remote Metadata Type setting
     * @param client The OpenSearch node client used as the default implementation
     * @param xContentRegistry The OpenSearch XContentRegistry
     * @param metadataSettings A map defining the remote metadata type and configuration
     * @return An instance of SdkClient which delegates to an implementation based on Remote Metadata Type
     */
    public static SdkClient createSdkClient(Client client, NamedXContentRegistry xContentRegistry, Map<String, String> metadataSettings) {
        String remoteMetadataType = metadataSettings.get(REMOTE_METADATA_TYPE_KEY);
        Boolean multiTenancy = Boolean.parseBoolean(metadataSettings.get(TENANT_AWARE_KEY));

        ServiceLoader<SdkClientDelegate> loader = ServiceLoader.load(SdkClientDelegate.class);
        Iterator<SdkClientDelegate> iterator = loader.iterator();

        if (Strings.isNullOrEmpty(remoteMetadataType)) {
            // Default client does not use SPI
            log.info("Using local opensearch cluster as metadata store.", remoteMetadataType);
            return createDefaultClient(client, xContentRegistry, metadataSettings, multiTenancy);
        } else {
            // Use SPI to find the correct client
            while (iterator.hasNext()) {
                SdkClientDelegate delegate = iterator.next();
                if (delegate.supportsMetadataType(remoteMetadataType)) {
                    log.info("Using {} as metadata store.", remoteMetadataType);
                    delegate.initialize(metadataSettings);
                    return new SdkClient(delegate, multiTenancy);
                }
            }
        }

        // If no suitable implementation is found, use the default
        log.warn("Unable to find {} client implementation. Using local opensearch cluster as metadata store.", remoteMetadataType);
        return createDefaultClient(client, xContentRegistry, metadataSettings, multiTenancy);
    }

    private static SdkClient createDefaultClient(
        Client client,
        NamedXContentRegistry xContentRegistry,
        Map<String, String> metadataSettings,
        Boolean multiTenancy
    ) {
        LocalClusterIndicesClient defaultclient = new LocalClusterIndicesClient(client, xContentRegistry, metadataSettings);
        return new SdkClient(defaultclient, multiTenancy);
    }

    // Package private for testing
    static SdkClient wrapSdkClientDelegate(SdkClientDelegate delegate, Boolean multiTenancy) {
        return new SdkClient(delegate, multiTenancy);
    }
}
