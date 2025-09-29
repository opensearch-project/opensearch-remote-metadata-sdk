## Version 3.3.0.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.3.0

### Features
- Add global resource support ([#224](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/224))

### Enhancements
- Add SeqNo and PrimaryTerm support to Put and Delete requests ([#234](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/234))
- Add RefreshPolicy and timeout support to Put, Update, Delete, and Bulk requests ([#244](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/244))

### Bug Fixes
- Throw exception on empty string for put request ID ([#235](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/235))

### Refactoring
- Remove unneeded enum uppercase workaround ([#185](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/185))
- Update argument type for ThreadContextAccess:doPrivileged ([#250](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/250))
- Use AccessController instead of ThreadContextAccess as it's for internal use ([#254](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/254))
