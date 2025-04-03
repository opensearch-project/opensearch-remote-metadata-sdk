# Remote Metadata SdkClient Developer Guide

## Overview

This guide explains how to use the OpenSearch Remote Metadata SDK in a plugin. Follow these steps to integrate the SDK:

1. Declare dependencies
2. Initialize the SDK Client
3. Inject the client into required classes
4. Migrate calls from NodeClient to SdkClient
5. Update exception handling
6. (Optional) Handle multitenancy

## 1. Declare Dependencies

#### Core SDK and Default Client

Add the following to your `build.gradle`:

```groovy
implementation ("org.opensearch:opensearch-remote-metadata-sdk:${opensearch_build}")
```

#### Remote Store Clients

For remote storage, import the appropriate client:

- `remote-client`: Use a remote OpenSearch cluster
- `aos-client`: Use Amazon OpenSearch Service (AOS) or Amazon OpenSearch Serverless (AOSS)
- `ddb-client`: Use DynamoDB for CRUD operations and AOS or AOSS for search (requires [zero-ETL replication](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/OpenSearchIngestionForDynamoDB.html))

Example for DynamoDB client:

```groovy
api ("org.opensearch:opensearch-remote-metadata-sdk:${opensearch_build}")
implementation ("org.opensearch:opensearch-remote-metadata-sdk-ddb-client:${opensearch_build}")
```

## 2. Initialize the SDK Client

Use the `SdkClientFactory` class to instantiate the client in your plugin's `createComponents()` method:

```java
import static org.opensearch.remote.metadata.common.CommonValue.*;

@Override
public Collection<Object> createComponents(
    Client client,
    ClusterService clusterService,
    ThreadPool threadPool,
    ResourceWatcherService resourceWatcherService,
    ScriptService scriptService,
    NamedXContentRegistry xContentRegistry,
    Environment environment,
    NodeEnvironment nodeEnvironment,
    NamedWriteableRegistry namedWriteableRegistry,
    IndexNameExpressionResolver indexNameExpressionResolver,
    Supplier<RepositoriesService> repositoriesServiceSupplier
) {
    Settings settings = environment.settings();
    SdkClient sdkClient = SdkClientFactory.createSdkClient(
        client,
        xContentRegistry,
        Map.ofEntries(
            // Assumes you have these values in plugin settings
            Map.entry(REMOTE_METADATA_TYPE_KEY, REMOTE_METADATA_TYPE.get(settings)),
            Map.entry(REMOTE_METADATA_ENDPOINT_KEY, REMOTE_METADATA_ENDPOINT.get(settings)),
            Map.entry(REMOTE_METADATA_REGION_KEY, REMOTE_METADATA_REGION.get(settings)),
            Map.entry(REMOTE_METADATA_SERVICE_NAME_KEY, REMOTE_METADATA_SERVICE_NAME.get(settings)),
            Map.entry(TENANT_AWARE_KEY, "true"), // Set to "false" if multitenancy is not needed
            Map.entry(TENANT_ID_FIELD_KEY, TENANT_ID_FIELD) // "tenant_id" field added in documents with multitenancy
        ),
        // Placeholder. Currently unused with non-blocking client implementations but a thread pool may be needed in the future
        client.threadPool().executor(ThreadPool.Names.GENERIC)
    );
    // Other existing code in this method. Pass sdkClient to any other classes that may need it
    return List.of(
        // other existing objects to be injected
        sdkClient
    );
}
```

## 3. Inject the Client in Required Classes

```java
@Inject
public FooTransportAction(
    // other existing arguments
    SdkClient sdkClient
) { ... }
```

## 4. Migrate Calls from NodeClient to SdkClient

Replace Request classes with their SDK wrapper equivalents:

| NodeClient | SDK Client |
|------------|------------|
| IndexRequest | PutDataObjectRequest |
| GetRequest | GetDataObjectRequest |
| UpdateRequest | UpdateDataObjectRequest |
| DeleteRequest | DeleteDataObjectRequest |
| SearchRequest | SearchDataObjectRequest |

Example of migrating a `get` operation:

```java
// Assume an ActionListener<GetResponse> actionListener

// Old NodeClient code
GetRequest getRequest = new GetRequest(indexName, documentId);
client.get(getRequest, actionListener);

// New SdkClient code
GetDataObjectRequest getDataObjectRequest = GetDataObjectRequest.builder()
    .index(indexName)
    .id(documentId)
    .build();
sdkClient.getDataObjectAsync(getDataObjectRequest)
    .whenComplete(SdkClientUtils.wrapGetCompletion(actionListener));
```

Additional wrappers are available for the other operations, and finer control over which exceptions to unwrap is available.

## 5. Update Exception Handling

Expand NodeClient-specific exception checks with more generic status checks:

```java
// Old code
if (e instanceof VersionConflictEngineException)

// New code
if (e instanceof VersionConflictEngineException || (e instanceof OpenSearchException && ((OpenSearchException) e).status() == RestStatus.CONFLICT))
```

## 6. Optional: Handle Multitenancy

If multitenancy is enabled:

1. Extract the Tenant ID from the REST Request header
2. Pass the Tenant ID through your code as needed
3. Validate that the tenant ID exists and matches the document tenant ID when required
4. Add `.tenantId(tenantId)` to your request objects

For examples of multitenancy implementation, refer to the ML Commons and Flow Framework plugins.
```

## Other notes

This client does not create indices (on OpenSearch) or tables (on DynamoDB). You will need to manually create those.
