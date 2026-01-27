use std::path::{Path, PathBuf};

use anyhow::anyhow;

use sha2::{Digest, Sha256, digest::KeyInit};
use tokio::fs;
use tracing::{debug, error, info, trace, warn};

#[derive(Debug, Clone)]
pub struct DiskStorage {
    pub base_dir: PathBuf,
}

impl DiskStorage {
    pub async fn new<P: AsRef<Path>>(base_dir: P) -> anyhow::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        debug!("Initializing disk storage at: {:?}", base_dir);

        if !base_dir.exists() {
            info!("Creating cache directory: {:?}", base_dir);
            fs::create_dir_all(&base_dir).await?;
        } else {
            debug!("Cache directory already exists: {:?}", base_dir);
        }

        Ok(Self { base_dir })
    }

    pub fn key_to_filename(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let hash_result = hasher.finalize();

        hex::encode(hash_result)
    }

    pub async fn store(&self, key: &str, data: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);
        let data_bytes = data.as_ref();

        trace!(
            "Storing {} bytes for key '{}' to file: {:?}",
            data_bytes.len(),
            key,
            filepath
        );

        tokio::fs::write(filepath, data_bytes).await?;
        debug!(
            "Successfully stored data for key '{}', size: {} bytes",
            key,
            data_bytes.len()
        );
        Ok(())
    }

    pub async fn load(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);

        trace!("Loading data for key '{}' from file: {:?}", key, filepath);

        if !filepath.exists() {
            trace!("File does not exist for key '{}': {:?}", key, filepath);
            return Err(anyhow!("file {} does not exist", filepath.display()));
        }

        match tokio::fs::read(filepath).await {
            Ok(data) => {
                debug!(
                    "Successfully loaded data for key '{}', size: {} bytes",
                    key,
                    data.len()
                );
                Ok(data)
            }
            Err(e) => {
                error!("Failed to load data for key '{}': {}", key, e);
                Err(e.into())
            }
        }
    }

    #[allow(dead_code)]
    pub async fn remove(&self, key: &str) -> anyhow::Result<()> {
        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);

        trace!("Removing file for key '{}': {:?}", key, filepath);

        if !filepath.exists() {
            warn!(
                "Attempted to remove non-existent file for key '{}': {:?}",
                key, filepath
            );
            return Err(anyhow!("file {} does not exist", filepath.display()));
        }

        match tokio::fs::remove_file(filepath).await {
            Ok(_) => {
                debug!("Successfully removed file for key '{}'", key);
                Ok(())
            }
            Err(e) => {
                error!("Failed to remove file for key '{}': {}", key, e);
                Err(e.into())
            }
        }
    }
}
