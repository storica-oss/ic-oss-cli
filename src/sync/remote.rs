use super::types::{join_path, EntryKind, RemoteEntry, RemoteManifest};
use ic_oss::bucket::Client;
use ic_oss_types::{
    entry::{EntryInfoV2, EntryKind as ApiEntryKind, SyncError},
    file::FileInfo,
    folder::FolderInfo,
    storage::{ManifestEntry, SubtreeManifestInput},
};
use std::collections::{HashSet, VecDeque};

const MANIFEST_PAGE_SIZE: u16 = 1000;
const MANIFEST_SCAN_ATTEMPTS: u8 = 3;

pub async fn scan_remote(
    client: &Client,
    remote_parent: u32,
    root_status: i8,
    use_manifest: bool,
) -> Result<RemoteManifest, String> {
    if use_manifest {
        return scan_remote_manifest(client, remote_parent, root_status).await;
    }
    scan_remote_legacy(client, remote_parent, root_status).await
}

async fn scan_remote_manifest(
    client: &Client,
    remote_parent: u32,
    root_status: i8,
) -> Result<RemoteManifest, String> {
    for attempt in 1..=MANIFEST_SCAN_ATTEMPTS {
        match scan_remote_manifest_once(client, remote_parent, root_status).await {
            Ok(manifest) => return Ok(manifest),
            Err(SyncError::Conflict { message, .. }) if attempt < MANIFEST_SCAN_ATTEMPTS => {
                println!(
                    "remote manifest changed during scan ({message}); retry {attempt}/{}",
                    MANIFEST_SCAN_ATTEMPTS - 1
                );
            }
            Err(err) => return Err(format!("subtree manifest scan failed: {err:?}")),
        }
    }
    unreachable!("manifest scan loop always returns")
}

async fn scan_remote_manifest_once(
    client: &Client,
    remote_parent: u32,
    root_status: i8,
) -> Result<RemoteManifest, SyncError> {
    let mut manifest = RemoteManifest {
        root_status,
        ..Default::default()
    };
    let mut cursor = None;
    loop {
        let page = client
            .get_subtree_manifest(SubtreeManifestInput {
                root: remote_parent,
                cursor,
                take: Some(MANIFEST_PAGE_SIZE),
            })
            .await?;
        let entry_count = page.entries.len();
        for item in page.entries {
            insert_manifest_entry(&mut manifest, item);
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
        if entry_count == 0 {
            return Err(SyncError::Internal(
                "subtree manifest returned an empty page with a continuation cursor".to_string(),
            ));
        }
    }
    Ok(manifest)
}

fn insert_manifest_entry(manifest: &mut RemoteManifest, item: ManifestEntry) {
    let path = item.path;
    if path.is_empty() {
        manifest
            .conflicts
            .push((path, "remote manifest contains an empty path".to_string()));
        return;
    }
    let Some(entry) = remote_entry(path.clone(), item.entry, &mut manifest.conflicts) else {
        return;
    };
    if let Some(existing) = manifest.entries.get(&path) {
        manifest.conflicts.push((
            path,
            format!(
                "duplicate remote manifest path (ids {} and {})",
                existing.id, entry.id
            ),
        ));
        return;
    }
    manifest.entries.insert(path, entry);
}

fn remote_entry(
    path: String,
    entry: EntryInfoV2,
    conflicts: &mut Vec<(String, String)>,
) -> Option<RemoteEntry> {
    let kind = match entry.kind {
        ApiEntryKind::Folder => EntryKind::Directory,
        ApiEntryKind::File => EntryKind::File,
    };
    let (size, filled) = match entry.kind {
        ApiEntryKind::Folder => (0, 0),
        ApiEntryKind::File => match (entry.size, entry.filled) {
            (Some(size), Some(filled)) => (size, filled),
            _ => {
                conflicts.push((
                    path,
                    format!("remote file {} is missing size metadata", entry.id),
                ));
                return None;
            }
        },
    };
    if kind == EntryKind::File && filled != size {
        conflicts.push((
            path.clone(),
            format!(
                "remote file {} is incomplete ({}/{})",
                entry.id, filled, size
            ),
        ));
    }
    Some(RemoteEntry {
        path,
        id: entry.id,
        parent: entry.parent,
        kind,
        size,
        filled,
        hash: entry.hash.map(|hash| *hash),
        status: entry.status,
    })
}

async fn scan_remote_legacy(
    client: &Client,
    remote_parent: u32,
    root_status: i8,
) -> Result<RemoteManifest, String> {
    let mut manifest = RemoteManifest {
        root_status,
        ..Default::default()
    };
    let mut queue = VecDeque::from([(remote_parent, String::new())]);
    let mut visited = HashSet::from([remote_parent]);

    while let Some((parent_id, parent_path)) = queue.pop_front() {
        for folder in list_all_folders(client, parent_id).await? {
            let path = join_path(&parent_path, &folder.name);
            let entry = RemoteEntry {
                path: path.clone(),
                id: folder.id,
                parent: folder.parent,
                kind: EntryKind::Directory,
                size: 0,
                filled: 0,
                hash: None,
                status: folder.status,
            };
            if let Some(existing) = manifest.entries.get(&path) {
                manifest.conflicts.push((
                    path,
                    format!(
                        "duplicate remote name (ids {} and {})",
                        existing.id, folder.id
                    ),
                ));
                continue;
            }
            manifest.entries.insert(path.clone(), entry);
            if !visited.insert(folder.id) {
                manifest.conflicts.push((
                    path,
                    format!("remote folder cycle or duplicate id {}", folder.id),
                ));
                continue;
            }
            queue.push_back((folder.id, path));
        }

        for file in list_all_files(client, parent_id).await? {
            let path = join_path(&parent_path, &file.name);
            let entry = RemoteEntry {
                path: path.clone(),
                id: file.id,
                parent: file.parent,
                kind: EntryKind::File,
                size: file.size,
                filled: file.filled,
                hash: file.hash.map(|hash| *hash),
                status: file.status,
            };
            if let Some(existing) = manifest.entries.get(&path) {
                manifest.conflicts.push((
                    path,
                    format!(
                        "duplicate remote name (ids {} and {})",
                        existing.id, file.id
                    ),
                ));
                continue;
            }
            if file.filled != file.size {
                manifest.conflicts.push((
                    path.clone(),
                    format!(
                        "remote file {} is incomplete ({}/{})",
                        file.id, file.filled, file.size
                    ),
                ));
            }
            manifest.entries.insert(path, entry);
        }
    }

    Ok(manifest)
}

async fn list_all_files(client: &Client, parent: u32) -> Result<Vec<FileInfo>, String> {
    let mut output = Vec::new();
    let mut previous = None;
    loop {
        let page = client.list_files(parent, previous, Some(100)).await?;
        if page.is_empty() {
            break;
        }
        previous = page.last().map(|file| file.id);
        output.extend(page);
    }
    Ok(output)
}

async fn list_all_folders(client: &Client, parent: u32) -> Result<Vec<FolderInfo>, String> {
    let mut output = Vec::new();
    let mut previous = None;
    loop {
        let page = client.list_folders(parent, previous, Some(100)).await?;
        if page.is_empty() {
            break;
        }
        previous = page.last().map(|folder| folder.id);
        output.extend(page);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_bytes::ByteArray;

    fn api_entry(kind: ApiEntryKind, id: u32) -> EntryInfoV2 {
        EntryInfoV2 {
            kind,
            id,
            parent: 0,
            name: "entry".to_string(),
            created_at: 1,
            updated_at: 1,
            status: 0,
            revision: 1,
            size: (kind == ApiEntryKind::File).then_some(3),
            filled: (kind == ApiEntryKind::File).then_some(3),
            hash: (kind == ApiEntryKind::File).then_some(ByteArray::from([7; 32])),
            content_type: (kind == ApiEntryKind::File).then_some("text/plain".to_string()),
        }
    }

    #[test]
    fn manifest_entries_convert_and_report_invalid_remote_state() {
        let mut manifest = RemoteManifest::default();
        insert_manifest_entry(
            &mut manifest,
            ManifestEntry {
                path: "docs".to_string(),
                entry: api_entry(ApiEntryKind::Folder, 1),
            },
        );
        insert_manifest_entry(
            &mut manifest,
            ManifestEntry {
                path: "docs/readme.md".to_string(),
                entry: api_entry(ApiEntryKind::File, 2),
            },
        );
        assert_eq!(manifest.entries["docs"].kind, EntryKind::Directory);
        assert_eq!(manifest.entries["docs/readme.md"].hash, Some([7; 32]));

        let mut incomplete = api_entry(ApiEntryKind::File, 3);
        incomplete.filled = Some(2);
        insert_manifest_entry(
            &mut manifest,
            ManifestEntry {
                path: "broken.bin".to_string(),
                entry: incomplete,
            },
        );
        insert_manifest_entry(
            &mut manifest,
            ManifestEntry {
                path: "docs".to_string(),
                entry: api_entry(ApiEntryKind::Folder, 4),
            },
        );
        assert!(manifest
            .conflicts
            .iter()
            .any(|(path, reason)| path == "broken.bin" && reason.contains("incomplete")));
        assert!(manifest
            .conflicts
            .iter()
            .any(|(path, reason)| path == "docs" && reason.contains("duplicate")));
    }
}
