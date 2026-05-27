## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Enhancements

* Migrate core and ddb-client modules from Jackson 2.x to Jackson 3.x (`tools.jackson`) ([#358](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/358))

### Bug Fixes

* Fix CVE-2025-67030 by bumping plexus-utils from 3.3.0 to 3.6.1 to address directory traversal vulnerability ([#371](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/371))

### Infrastructure

* Add Gradle dependency caching to setup-java steps in CI workflows to reduce build times ([#364](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/364))

### Maintenance

* Add release notes for 3.6.0 ([#353](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/353))
