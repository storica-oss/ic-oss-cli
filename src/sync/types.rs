use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalEntry {
    pub path: String,
    pub kind: EntryKind,
    pub size: u64,
    pub hash: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Default)]
pub struct LocalManifest {
    pub entries: BTreeMap<String, LocalEntry>,
    pub protected_paths: BTreeSet<String>,
    pub warnings: Vec<String>,
    pub cache_hits: usize,
    pub hashed_files: usize,
}

impl LocalManifest {
    pub fn protects(&self, path: &str) -> bool {
        self.protected_paths.iter().any(|protected| {
            path == protected
                || path
                    .strip_prefix(protected)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteEntry {
    pub path: String,
    pub id: u32,
    pub parent: u32,
    pub kind: EntryKind,
    pub size: u64,
    pub filled: u64,
    pub hash: Option<[u8; 32]>,
    pub status: i8,
}

#[derive(Clone, Debug, Default)]
pub struct RemoteManifest {
    pub entries: BTreeMap<String, RemoteEntry>,
    pub root_status: i8,
    pub warnings: Vec<String>,
    pub conflicts: Vec<(String, String)>,
}

pub fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", parent, name)
    }
}

pub fn parent_path(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

pub fn path_depth(path: &str) -> usize {
    if path.is_empty() {
        0
    } else {
        path.bytes().filter(|byte| *byte == b'/').count() + 1
    }
}
