use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use dirs::cache_dir;
use tracing::{debug, info, trace, warn};

use crate::chuck::chunk_cache::{
    disk_storage::{self, DiskStorage},
    policy::Policy,
};

#[derive(Debug, Clone)]
pub struct ChunksCacheConfig {
    /// Maximum number of bytes in hot cache (fastest access tier)
    ///
    /// **Recommended**: 100MB-500MB depending on available memory
    /// **Impact**: Higher values = more hot data but more memory usage
    pub hot_cache_max_bytes: usize,

    /// Maximum number of bytes in cold cache (metadata tracking tier)
    ///
    /// **Recommended**: Same as hot_cache_max_bytes or 2x for comprehensive tracking
    /// **Impact**: Higher values = better access pattern visibility but more metadata overhead
    pub cold_cache_max_bytes: usize,

    /// Base access frequency threshold for promoting items to hot cache
    ///
    /// **Units**: accesses per second
    /// **Typical Range**: 5.0 - 20.0
    /// **Lower Values**: More aggressive promotion (better for bursty workloads)
    /// **Higher Values**: More conservative (better for stable workloads)
    pub base_promotion_threshold: f64,

    /// Short time window for burst access detection
    ///
    /// **Purpose**: Capture rapid, recent access patterns
    /// **Recommended**: 5-15 seconds
    /// **Trade-off**: Shorter = more responsive but noisier
    pub short_window_size: Duration,

    /// Medium time window for trend analysis
    ///
    /// **Purpose**: Identify sustained access patterns and trends
    /// **Recommended**: 30-120 seconds
    /// **Trade-off**: Longer = more stable but slower to adapt
    pub medium_window_size: Duration,

    /// Maximum number of access records to keep per key
    ///
    /// **Note**: Legacy parameter for compatibility. Actual bucket count is
    ///         calculated dynamically based on window sizes.
    pub max_access_entries: usize,

    /// Custom disk storage directory (optional)
    ///
    /// If None, uses system cache directory via `dirs::cache_dir()`
    /// Files are stored using SHA256 hash of the key for unique naming
    pub disk_storage_dir: Option<PathBuf>,

    /// Weight for short window access frequency in promotion decisions
    ///
    /// **Range**: 0.0 - 1.0
    /// **Higher Values**: Prioritize recent burst access (good for interactive workloads)
    /// **Lower Values**: Prioritize sustained trends (good for batch workloads)
    pub short_window_weight: f64,

    /// Weight for medium window access frequency in promotion decisions
    ///
    /// **Range**: 0.0 - 1.0
    /// **Note**: short_window_weight + medium_window_weight should typically sum to 1.0
    pub medium_window_weight: f64,

    /// Enable adaptive threshold adjustment based on system load and hit rate
    ///
    /// **When true**: Dynamically adjusts promotion threshold based on:
    ///   - System load (cache utilization + request rate)
    ///   - Cache hit rate
    ///
    /// **When false**: Uses fixed base_promotion_threshold
    pub enable_adaptive_threshold: bool,

    /// System load threshold for triggering aggressive promotion mode
    ///
    /// **Range**: 0.0 - 1.0
    /// **When exceeded**: Reduces promotion threshold by 30% to cache more data
    /// **Purpose**: Improve performance under high load by increasing cache hit rate
    pub aggressive_promotion_load_threshold: f64,

    /// Cache hit rate threshold for triggering conservative promotion mode
    ///
    /// **Range**: 0.0 - 1.0
    /// **When below**: Increases promotion threshold by 30% to prevent cache pollution
    /// **Purpose**: Maintain cache efficiency when hit rate is already low
    pub conservative_promotion_hit_rate_threshold: f64,
}

impl Default for ChunksCacheConfig {
    fn default() -> Self {
        Self {
            hot_cache_max_bytes: 100 * 1024 * 1024,
            cold_cache_max_bytes: 100 * 1024 * 1024,
            base_promotion_threshold: 10.0,
            short_window_size: Duration::from_secs(10),
            medium_window_size: Duration::from_secs(60),
            max_access_entries: 100,
            disk_storage_dir: None,
            short_window_weight: 0.7,
            medium_window_weight: 0.3,
            enable_adaptive_threshold: true,
            aggressive_promotion_load_threshold: 0.8,
            conservative_promotion_hit_rate_threshold: 0.6,
        }
    }
}

#[derive(Clone)]
pub struct ChunksCache {
    /// Persistent disk storage backend with SHA256-based file naming
    disk_storage: DiskStorage,

    /// Hot cache tier storing frequently accessed data in memory
    /// Uses Moka's high-performance concurrent cache implementation
    hot_cache: moka::future::Cache<String, Vec<u8>>,

    /// Cold cache tier tracking all accessed keys for pattern analysis
    /// Stores empty tuples () as lightweight metadata markers
    cold_cache: moka::future::Cache<String, usize>,

    /// Intelligent promotion policy engine with adaptive thresholding
    policy: Policy,

    /// Cache configuration parameters (stored for runtime adjustments)
    config: ChunksCacheConfig,
}

impl ChunksCache {
    /// Creates a new ChunksCache with default configuration
    #[allow(dead_code)]
    pub async fn new() -> anyhow::Result<Self> {
        Self::new_with_config(ChunksCacheConfig::default()).await
    }

    /// Creates a new ChunksCache with custom configuration
    pub async fn new_with_config(mut config: ChunksCacheConfig) -> anyhow::Result<Self> {
        info!(
            "Creating new ChunksCache with configuration: hot_cache_size={}, cold_cache_size={}, base_promotion_threshold={}",
            config.hot_cache_max_bytes,
            config.cold_cache_max_bytes,
            config.base_promotion_threshold
        );

        let cache_dir = config
            .disk_storage_dir
            .take()
            .unwrap_or_else(|| cache_dir().unwrap());
        debug!("Using cache directory: {:?}", cache_dir);
        let disk_storage = DiskStorage::new(cache_dir).await?;

        let hot_bytes = Arc::new(AtomicU64::new(0));
        let hot_bytes_evict = hot_bytes.clone();
        let hot_cache_builder = moka::future::Cache::builder()
            .weigher(|_: &String, v: &Vec<u8>| v.len() as u32)
            .max_capacity(config.hot_cache_max_bytes as u64)
            .time_to_idle(Duration::from_secs(30))
            .time_to_live(Duration::from_secs(120))
            .eviction_listener(move |_key, value: Vec<u8>, _cause| {
                hot_bytes_evict.fetch_sub(value.len() as u64, Ordering::Relaxed);
            });
        let cold_cache_builder = moka::future::Cache::builder()
            .weigher(|_: &String, v: &usize| *v as u32)
            .max_capacity(config.cold_cache_max_bytes as u64)
            .time_to_idle(Duration::from_secs(30))
            .time_to_live(Duration::from_secs(120));

        debug!(
            "Creating policy with adaptive threshold: {}",
            config.enable_adaptive_threshold
        );
        let policy = Policy::new(
            config.short_window_size,
            config.medium_window_size,
            config.max_access_entries,
            config.base_promotion_threshold,
            config.short_window_weight,
            config.medium_window_weight,
            config.enable_adaptive_threshold,
            config.aggressive_promotion_load_threshold,
            config.conservative_promotion_hit_rate_threshold,
        );

        info!("ChunksCache created successfully");
        Ok(Self {
            disk_storage,
            hot_cache: hot_cache_builder.build(),
            cold_cache: cold_cache_builder.build(),
            policy,
            config,
        })
    }

    pub async fn get(&self, key: &String) -> Option<Vec<u8>> {
        trace!("Cache GET request for key: {}", key);
        self.policy.record_access(key).await;

        // Check hot cache first
        if let Some(value) = self.hot_cache.get(key).await {
            debug!(
                "Hot cache HIT for key: {}, size: {} bytes",
                key,
                value.len()
            );
            self.policy.record_cache_request(true);
            self.update_utilization_metrics();
            return Some(value);
        }

        debug!("Hot cache MISS for key: {}", key);
        self.policy.record_cache_request(false);

        let diskstorage = self.disk_storage.clone();
        let policy = self.policy.clone();
        let hot_cache = self.hot_cache.clone();
        // ensure key exists in cold cache
        self.cold_cache.get(key).await?;

        let load_future = async move {
            trace!("Loading data from disk for key: {}", key);
            let value = diskstorage.load(key).await.unwrap_or_else(|e| {
                debug!("Disk load failed for key {}: {}", key, e);
                Vec::new()
            });

            if value.is_empty() {
                warn!("No data found on disk for key: {}", key);
                return value;
            }

            debug!("Loaded {} bytes from disk for key: {}", value.len(), key);

            if policy.should_promote(key).await {
                debug!("Promoting key to hot cache: {}", key);
                hot_cache.insert(key.clone(), value.clone()).await;
            } else {
                trace!("Key not eligible for promotion: {}", key);
            }

            value
        };

        let value = self.hot_cache.get_with_by_ref(key, load_future).await;

        self.update_utilization_metrics();

        if value.is_empty() {
            debug!("No data found for key: {}", key);
            None
        } else {
            debug!("Returning {} bytes for key: {}", value.len(), key);
            value.into()
        }
    }

    /// Update cache utilization metrics
    fn update_utilization_metrics(&self) {
        let current_size = self.hot_cache.entry_count();
        let max_size = self.config.hot_cache_max_bytes as u64;
        trace!(
            "Updating cache utilization: {}/{} entries, {} MB hot bytes",
            current_size,
            max_size,
            self.hot_cache.weighted_size() / 1024 / 1024
        );
        self.policy.update_cache_utilization(current_size, max_size);
    }

    pub async fn insert(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        info!(
            "Cache INSERT request for key: {}, size: {} bytes",
            key,
            data.len()
        );
        trace!("Inserting into hot cache: {}", key);
        self.hot_cache.insert(key.to_owned(), data.to_owned()).await;

        let key_ownd = key.to_string();
        let data = data.to_owned();
        let disk_storage = self.disk_storage.clone();
        let cold_cache = self.cold_cache.clone();

        tokio::spawn(async move {
            trace!("Storing on disk: {}", key_ownd);
            if let Err(e) = disk_storage.store(&key_ownd, &data).await {
                {
                    warn!("Failed to store in disk: {e}");
                    return;
                }
            }

            trace!("Adding to cold cache: {}", key_ownd);
            cold_cache.insert(key_ownd.to_owned(), data.len()).await;
        });

        debug!("Successfully inserted key: {}", key);
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn remove(&self, key: &String) -> anyhow::Result<()> {
        info!("Cache REMOVE request for key: {}", key);
        trace!("Invalidating from hot cache: {}", key);
        self.hot_cache.invalidate(key).await;
        trace!("Invalidating from cold cache: {}", key);
        self.cold_cache.invalidate(key).await;

        debug!("Successfully removed key: {}", key);
        Ok(())
    }
}
