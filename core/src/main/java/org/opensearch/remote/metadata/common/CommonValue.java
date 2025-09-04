/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.common;

import java.util.Set;

/**
 * Strings used by classes depending on this library
 */
public class CommonValue {
    /** The key for tenant id field name. */
    public static final String TENANT_ID_FIELD_KEY = "tenant_id";
    /** The key for tenant awareness. */
    public static final String TENANT_AWARE_KEY = "tenant_aware";
    /** The key for remote metadata type. */
    public static final String REMOTE_METADATA_TYPE_KEY = "remote_metadata_type";
    /** The key for remote metadata endpoint, applicable to remote clusters or Zero-ETL DynamoDB sinks */
    public static final String REMOTE_METADATA_ENDPOINT_KEY = "remote_metadata_endpoint";
    /** The key for remote metadata region, applicable for AWS SDK v2 connections */
    public static final String REMOTE_METADATA_REGION_KEY = "remote_metadata_region";
    /** The key for remote metadata service name used by service-specific SDKs */
    public static final String REMOTE_METADATA_SERVICE_NAME_KEY = "remote_metadata_service_name";
    /** A configurable tenant id to identify global resources. */
    public static final String REMOTE_METADATA_GLOBAL_TENANT_ID_KEY = "global_tenant_id";
    /** The key for remote metadata cache refresh interval */
    public static final String REMOTE_METADATA_GLOBAL_RESOURCE_CACHE_TTL_MILLIS_KEY = "remote_metadata_global_resource_cache_millis_ttl";

    /** The value for remote metadata type for a remote OpenSearch cluster compatible with OpenSearch Java Client. */
    public static final String REMOTE_OPENSEARCH = "RemoteOpenSearch";
    /** The value for remote metadata type for a remote cluster on AWS DynamoDB and Zero-ETL replication to OpenSearch */
    public static final String AWS_DYNAMO_DB = "AWSDynamoDB";
    /** The value for remote metadata type for a remote cluster on AWS OpenSearch Service using AWS SDK v2. */
    public static final String AWS_OPENSEARCH_SERVICE = "AWSOpenSearchService";
    private static final String AOS_SERVICE_NAME = "es";
    private static final String AOSS_SERVICE_NAME = "aoss";
    /** Service Names compatible with AWS SDK v2. */
    public static final Set<String> VALID_AWS_OPENSEARCH_SERVICE_NAMES = Set.of(AOS_SERVICE_NAME, AOSS_SERVICE_NAME);
}
