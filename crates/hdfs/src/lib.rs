use datafusion_objectstore_hdfs::object_store::hdfs::HadoopFileSystem;
use std::sync::Arc;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{
    factories, url_prefix_handler, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::ObjectStore;
use url::Url;

pub mod error;

#[derive(Clone, Default, Debug)]
pub struct HdfsFactory {}

impl ObjectStoreFactory for HdfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        _options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let hdfs = HadoopFileSystem::new(url.as_str()).ok_or_else(|| {
            DeltaTableError::Generic(format!(
                "Failed to create HadoopFileSystem from url: {}",
                url
            ))
        })?;
        let store: Box<dyn ObjectStore> = Box::new(hdfs) as _;
        let prefix = Path::from_url_path(url.path())?;
        Ok((url_prefix_handler(store, prefix.clone()), prefix))
    }
}

impl LogStoreFactory for HdfsFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common HDFS [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(HdfsFactory {});
    let scheme = &"hdfs";
    let url = Url::parse(&format!("{}://", scheme)).unwrap();
    factories().insert(url.clone(), factory.clone());
    logstores().insert(url.clone(), factory.clone());
}
