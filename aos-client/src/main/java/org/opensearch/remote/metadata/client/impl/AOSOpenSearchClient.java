/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.core.common.Strings;
import org.opensearch.remote.metadata.client.SdkClient;

import java.util.Map;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.remote.metadata.common.CommonValue.AWS_OPENSEARCH_SERVICE;
import static org.opensearch.remote.metadata.common.CommonValue.VALID_AWS_OPENSEARCH_SERVICE_NAMES;

/**
 * An implementation of {@link SdkClient} that stores data in a remote
 * OpenSearch cluster using the OpenSearch Java Client.
 */
public class AOSOpenSearchClient extends RemoteClusterIndicesClient {

    @Override
    public boolean supportsMetadataType(String metadataType) {
        return AWS_OPENSEARCH_SERVICE.equals(metadataType);
    }

    @Override
    public void initialize(Map<String, String> metadataSettings) {
        super.initialize(metadataSettings);
        this.openSearchClient = createOpenSearchClient();
        this.mapper = openSearchClient._transport().jsonpMapper();
    }

    /**
     * Empty constructor for SPI
     */
    public AOSOpenSearchClient() {}

    private void validateAwsParams() {
        if (Strings.isNullOrEmpty(remoteMetadataEndpoint) || Strings.isNullOrEmpty(region)) {
            throw new org.opensearch.OpenSearchException(remoteMetadataType + " client requires a metadata endpoint and region.");
        }
        if (!VALID_AWS_OPENSEARCH_SERVICE_NAMES.contains(serviceName)) {
            throw new org.opensearch.OpenSearchException(
                remoteMetadataType + " client only supports service names " + VALID_AWS_OPENSEARCH_SERVICE_NAMES
            );
        }
    }

    @Override
    protected OpenSearchClient createOpenSearchClient() {
        validateAwsParams();
        return createAwsOpenSearchServiceClient();
    }

    private OpenSearchClient createAwsOpenSearchServiceClient() {
        // https://github.com/opensearch-project/opensearch-java/blob/main/guides/auth.md
        final SdkHttpClient httpClient = ApacheHttpClient.builder().build();
        return new OpenSearchClient(
            doPrivileged(
                () -> new AwsSdk2Transport(
                    httpClient,
                    remoteMetadataEndpoint.replaceAll("^https?://", ""), // OpenSearch endpoint, without https://
                    serviceName, // signing service name, use "es" for OpenSearch, "aoss" for OpenSearch Serverless
                    Region.of(region), // signing service region
                    AwsSdk2TransportOptions.builder().setCredentials(createCredentialsProvider()).build()
                )
            )
        );
    }

    private static AwsCredentialsProvider createCredentialsProvider() {
        return AwsCredentialsProviderChain.builder()
            .addCredentialsProvider(EnvironmentVariableCredentialsProvider.create())
            .addCredentialsProvider(ContainerCredentialsProvider.builder().build())
            .addCredentialsProvider(InstanceProfileCredentialsProvider.create())
            .build();
    }

    @Override
    public void close() throws Exception {
        if (openSearchClient != null && openSearchClient._transport() != null) {
            openSearchClient._transport().close();
        }
    }
}
