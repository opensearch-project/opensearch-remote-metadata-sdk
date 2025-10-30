

## Version 2.19.4 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 2.19.4

### Bug Fixes
* Avoid race condition putting the same document id in DDB Client ([#229](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/229))
* Throw exception on empty string for put request ID ([#237](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/237))

### Maintenance
* Exclude commons-lang3 from checkstyle transitive dependencies ([#275](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/275)
* Resolve netty CVEs([#277](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/277))
