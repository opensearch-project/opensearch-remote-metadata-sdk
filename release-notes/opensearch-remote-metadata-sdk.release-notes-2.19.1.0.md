## 2025-02-27 Version 2.19.1.0

Compatible with OpenSearch 2.19.1

### Bug Fixes
* Improve exception unwrapping flexibility for SdkClientUtils
* Add util methods to handle ActionListeners in whenComplete
* Refactor SDKClientUtil for better readability, fix javadocs
* Make DynamoDBClient fully async

### Maintenance
* Fix CVE-2025-24970 ([93](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/93))