use super::{
    persistence,
    types::{join_path, EntryKind, LocalEntry, LocalManifest},
};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ic_oss_types::file::valid_file_name;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

const HASH_CACHE_VERSION: u8 = 1;

#[derive(Debug, Default, Deserialize, Serialize)]
struct HashCache {
    version: u8,
    entries: BTreeMap<String, CachedHash>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CachedHash {
    size: u64,
    modified_secs: u64,
    modified_nanos: u32,
    hash: String,
}

pub fn scan_local(
    root: &Path,
    excludes: &[String],
    max_file_size: u64,
    max_folder_depth: usize,
    parent_depth: usize,
    max_children: usize,
) -> Result<LocalManifest, String> {
    let root = root
        .canonicalize()
        .map_err(|err| format!("failed to resolve local root {:?}: {}", root, err))?;
    if !root.is_dir() {
        return Err(format!("local sync path is not a directory: {:?}", root));
    }

    let cache_path = hash_cache_path(&root);
    scan_local_with_cache(
        &root,
        excludes,
        max_file_size,
        max_folder_depth,
        parent_depth,
        max_children,
        cache_path.as_deref(),
    )
}

#[allow(clippy::too_many_arguments)]
fn scan_local_with_cache(
    root: &Path,
    excludes: &[String],
    max_file_size: u64,
    max_folder_depth: usize,
    parent_depth: usize,
    max_children: usize,
    cache_path: Option<&Path>,
) -> Result<LocalManifest, String> {
    let excludes = compile_excludes(excludes)?;
    let (cache, cache_warning) = cache_path.map(load_hash_cache).unwrap_or_default();
    let mut next_cache = HashCache {
        version: HASH_CACHE_VERSION,
        entries: BTreeMap::new(),
    };
    let mut manifest = LocalManifest::default();
    if let Some(warning) = cache_warning {
        manifest.warnings.push(warning);
    }
    scan_directory(
        root,
        "",
        &excludes,
        max_file_size,
        max_folder_depth,
        parent_depth,
        max_children,
        &cache,
        &mut next_cache,
        &mut manifest,
    )?;
    if let Some(cache_path) = cache_path {
        if let Err(err) = save_hash_cache(cache_path, &next_cache) {
            manifest.warnings.push(err);
        }
    }
    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
fn scan_directory(
    directory: &Path,
    relative_parent: &str,
    excludes: &GlobSet,
    max_file_size: u64,
    max_folder_depth: usize,
    parent_depth: usize,
    max_children: usize,
    cache: &HashCache,
    next_cache: &mut HashCache,
    manifest: &mut LocalManifest,
) -> Result<(), String> {
    let mut entries = fs::read_dir(directory)
        .map_err(|err| format!("failed to read directory {:?}: {}", directory, err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("failed to read directory entry: {}", err))?;
    entries.sort_by_key(|entry| entry.file_name());

    let mut included_children = 0usize;
    for entry in entries {
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| format!("non-UTF-8 filename is not supported: {:?}", entry.path()))?;
        if !valid_file_name(&name) {
            return Err(format!("invalid filename for IC-OSS: {:?}", entry.path()));
        }

        let relative_path = join_path(relative_parent, &name);
        if is_excluded(excludes, &relative_path) {
            manifest.protected_paths.insert(relative_path);
            continue;
        }

        let file_type = entry
            .file_type()
            .map_err(|err| format!("failed to inspect {:?}: {}", entry.path(), err))?;
        if file_type.is_symlink() {
            manifest.protected_paths.insert(relative_path.clone());
            manifest
                .warnings
                .push(format!("skipped symbolic link {}", relative_path));
            continue;
        }

        if file_type.is_dir() {
            included_children += 1;
            let depth = parent_depth + super::types::path_depth(&relative_path);
            if depth > max_folder_depth {
                return Err(format!(
                    "folder depth exceeds bucket limit {}: {}",
                    max_folder_depth, relative_path
                ));
            }
            manifest.entries.insert(
                relative_path.clone(),
                LocalEntry {
                    path: relative_path.clone(),
                    kind: EntryKind::Directory,
                    size: 0,
                    hash: None,
                },
            );
            scan_directory(
                &entry.path(),
                &relative_path,
                excludes,
                max_file_size,
                max_folder_depth,
                parent_depth,
                max_children,
                cache,
                next_cache,
                manifest,
            )?;
        } else if file_type.is_file() {
            included_children += 1;
            let metadata_before = entry
                .metadata()
                .map_err(|err| format!("failed to inspect {:?}: {}", entry.path(), err))?;
            if metadata_before.len() > max_file_size {
                return Err(format!(
                    "file size {} exceeds bucket limit {}: {}",
                    metadata_before.len(),
                    max_file_size,
                    relative_path
                ));
            }

            let timestamp = modified_timestamp(&metadata_before);
            let cached_hash = timestamp.and_then(|(modified_secs, modified_nanos)| {
                cache.entries.get(&relative_path).and_then(|cached| {
                    if cached.size == metadata_before.len()
                        && cached.modified_secs == modified_secs
                        && cached.modified_nanos == modified_nanos
                    {
                        decode_hash(&cached.hash)
                    } else {
                        None
                    }
                })
            });
            let hash = if let Some(hash) = cached_hash {
                manifest.cache_hits += 1;
                hash
            } else {
                manifest.hashed_files += 1;
                hash_file(&entry.path())?
            };
            let metadata_after = entry
                .metadata()
                .map_err(|err| format!("failed to recheck {:?}: {}", entry.path(), err))?;
            if metadata_before.len() != metadata_after.len()
                || metadata_before.modified().ok() != metadata_after.modified().ok()
            {
                return Err(format!(
                    "local file changed while scanning: {}",
                    relative_path
                ));
            }

            if let Some((modified_secs, modified_nanos)) = timestamp {
                next_cache.entries.insert(
                    relative_path.clone(),
                    CachedHash {
                        size: metadata_before.len(),
                        modified_secs,
                        modified_nanos,
                        hash: hex::encode(hash),
                    },
                );
            }

            manifest.entries.insert(
                relative_path.clone(),
                LocalEntry {
                    path: relative_path,
                    kind: EntryKind::File,
                    size: metadata_before.len(),
                    hash: Some(hash),
                },
            );
        } else {
            manifest.protected_paths.insert(relative_path.clone());
            manifest.warnings.push(format!(
                "skipped unsupported filesystem entry {}",
                relative_path
            ));
        }
    }

    if (parent_depth > 0 || !relative_parent.is_empty()) && included_children > max_children {
        return Err(format!(
            "directory {} has {} children, exceeding bucket limit {}",
            relative_parent, included_children, max_children
        ));
    }
    Ok(())
}

fn modified_timestamp(metadata: &fs::Metadata) -> Option<(u64, u32)> {
    let duration = metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
    Some((duration.as_secs(), duration.subsec_nanos()))
}

fn decode_hash(value: &str) -> Option<[u8; 32]> {
    let bytes = hex::decode(value).ok()?;
    bytes.try_into().ok()
}

fn hash_cache_path(root: &Path) -> Option<PathBuf> {
    let mut hasher = Sha3_256::new();
    hasher.update(root.as_os_str().to_string_lossy().as_bytes());
    let key = hex::encode(hasher.finalize());
    Some(
        persistence::user_cache_dir()
            .join("sync-hashes")
            .join(format!("{key}.json")),
    )
}

fn load_hash_cache(path: &Path) -> (HashCache, Option<String>) {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return (HashCache::default(), None);
        }
        Err(err) => {
            return (
                HashCache::default(),
                Some(format!("ignored unreadable hash cache {:?}: {}", path, err)),
            );
        }
    };
    match serde_json::from_slice::<HashCache>(&bytes) {
        Ok(cache) if cache.version == HASH_CACHE_VERSION => (cache, None),
        Ok(_) => (
            HashCache::default(),
            Some(format!(
                "ignored unsupported hash cache version at {:?}",
                path
            )),
        ),
        Err(err) => (
            HashCache::default(),
            Some(format!("ignored invalid hash cache {:?}: {}", path, err)),
        ),
    }
}

fn save_hash_cache(path: &Path, cache: &HashCache) -> Result<(), String> {
    let bytes = serde_json::to_vec(cache)
        .map_err(|err| format!("failed to serialize hash cache: {}", err))?;
    persistence::atomic_write(path, &bytes)
}

fn is_excluded(excludes: &GlobSet, relative_path: &str) -> bool {
    excludes.is_match(relative_path) || excludes.is_match(format!("{}/", relative_path))
}

fn compile_excludes(patterns: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            Glob::new(pattern)
                .map_err(|err| format!("invalid exclude pattern {:?}: {}", pattern, err))?,
        );
    }
    builder
        .build()
        .map_err(|err| format!("failed to compile exclude patterns: {}", err))
}

fn hash_file(path: &Path) -> Result<[u8; 32], String> {
    let mut file = File::open(path).map_err(|err| format!("failed to open {:?}: {}", path, err))?;
    let mut hasher = Sha3_256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| format!("failed to read {:?}: {}", path, err))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn scans_empty_files_directories_and_excludes() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("ic-oss-sync-{}-{}", std::process::id(), suffix));
        fs::create_dir_all(root.join("docs")).unwrap();
        fs::write(root.join("empty.txt"), []).unwrap();
        fs::write(root.join("docs/readme.md"), b"hello").unwrap();
        fs::write(root.join("ignored.tmp"), b"ignored").unwrap();

        let cache = root.with_extension("cache.json");
        let manifest = scan_local_with_cache(
            &root,
            &["*.tmp".to_string()],
            1024,
            10,
            0,
            100,
            Some(&cache),
        )
        .unwrap();
        assert_eq!(manifest.entries["docs"].kind, EntryKind::Directory);
        assert_eq!(manifest.entries["empty.txt"].size, 0);
        assert!(manifest.entries["empty.txt"].hash.is_some());
        assert_eq!(manifest.entries["docs/readme.md"].size, 5);
        assert!(!manifest.entries.contains_key("ignored.tmp"));
        assert_eq!(manifest.hashed_files, 2);
        assert_eq!(manifest.cache_hits, 0);

        let cached = scan_local_with_cache(&root, &[], 1024, 10, 0, 100, Some(&cache)).unwrap();
        assert_eq!(cached.cache_hits, 2);
        assert_eq!(cached.hashed_files, 1);

        fs::remove_dir_all(root).unwrap();
        fs::remove_file(cache).unwrap();
    }
}
