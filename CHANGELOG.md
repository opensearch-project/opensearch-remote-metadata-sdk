# CHANGELOG
All notable changes to this project are documented in this file.

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased 3.3](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/compare/3.2...HEAD)
### Features
- Add global resource support ([#224](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/224))

### Enhancements
- Add SeqNo and PrimaryTerm support to Put and Delete requests ([#234](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/234))

### Bug Fixes
- Throw exception on empty string for put request ID ([#235](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/235))

### Infrastructure
### Documentation
### Maintenance
### Refactoring
- Remove unneeded enum uppercase workaround ([#185](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/185))
- Update argument type for ThreadContextAccess:doPrivileged ([#250](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/250))
- Use AccessController instead of ThreadContextAccess as it's for internal use ([#254](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/254))
