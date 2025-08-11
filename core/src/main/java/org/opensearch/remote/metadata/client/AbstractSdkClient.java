/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;

/**
 * Superclass abstracting privileged action execution
 */
public abstract class AbstractSdkClient implements SdkClientDelegate {

    protected String tenantIdField;

    protected String remoteMetadataType;
    protected String remoteMetadataEndpoint;
    protected String region;
    protected String serviceName;

    @Override
    public void initialize(Map<String, String> metadataSettings) {
        this.tenantIdField = metadataSettings.get(TENANT_ID_FIELD_KEY);

        this.remoteMetadataType = metadataSettings.get(REMOTE_METADATA_TYPE_KEY);
        this.remoteMetadataEndpoint = metadataSettings.get(REMOTE_METADATA_ENDPOINT_KEY);
        this.region = metadataSettings.get(REMOTE_METADATA_REGION_KEY);
        this.serviceName = metadataSettings.get(REMOTE_METADATA_SERVICE_NAME_KEY);
    }

    /**
     * Execute this priveleged action asynchronously
     * @param <T> The return type of the completable future to be returned
     * @param action the action to execute
     * @param executor the executor for the action
     * @return A {@link CompletionStage} encapsulating the completable future for the action
     */
    protected <T> CompletionStage<T> executePrivilegedAsync(PrivilegedAction<T> action, Executor executor) {
        return CompletableFuture.supplyAsync(() -> doPrivileged(action), executor);
    }

    /**
     * Determine if we should use the user-provided document id
     * @param id the document id
     * @return true if the id is not null
     * @throws IllegalArgumentException if the id is an empty string
     */
    protected boolean shouldUseId(String id) {
        if ("".equals(id)) {
            throw new IllegalArgumentException("if _id is specified it must not be empty");
        }
        return id != null;
    }
}
