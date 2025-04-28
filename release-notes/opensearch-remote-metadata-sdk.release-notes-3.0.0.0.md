## 2025-04-28 Version 3.0.0.0

Compatible with OpenSearch 3.0.0

### Bug Fixes
- Fix version conflict check for update ([#114](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/114))
- Use SdkClientDelegate's classloader for ServiceLoader ([#121](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/121))
- Ensure consistent reads on DynamoDB getItem calls ([#128](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/128))
- Return 404 for Index not found on Local Cluster search ([#130](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/130))
- Directly return responses from Local Cluster client ([#141](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/141))
- Make generated responses robust to URL encoded id and index values ([#156](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/156))
- Validate request fields in DDB Put and Update implementations ([#157](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/157))
- Properly handle remote client search failures with status codes ([#158](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/158))

### Documentation
- Add a developer guide ([#124](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/124))

### Refactoring
- Update o.o.client imports to o.o.transport.client ([#73](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/73))
