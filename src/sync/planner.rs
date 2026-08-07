use super::types::{parent_path, path_depth, EntryKind, LocalManifest, RemoteManifest};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Default)]
pub struct PlanOptions {
    pub delete: bool,
    pub overwrite: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanAction {
    Conflict {
        path: String,
        reason: String,
    },
    CreateDirectory {
        path: String,
    },
    UploadFile {
        path: String,
        size: u64,
    },
    ReplaceFile {
        path: String,
        remote_id: u32,
        size: u64,
    },
    DeleteFile {
        path: String,
        remote_id: u32,
    },
    DeleteDirectory {
        path: String,
        remote_id: u32,
    },
}

#[derive(Clone, Debug, Default)]
pub struct Plan {
    pub actions: Vec<PlanAction>,
    pub warnings: Vec<String>,
    pub create_directories: usize,
    pub upload_files: usize,
    pub replace_files: usize,
    pub delete_files: usize,
    pub delete_directories: usize,
    pub conflicts: usize,
    pub unchanged: usize,
    pub retained_remote: usize,
    pub upload_bytes: u64,
}

impl Plan {
    pub fn has_conflicts(&self) -> bool {
        self.conflicts > 0
    }

    fn push(&mut self, action: PlanAction) {
        match &action {
            PlanAction::Conflict { .. } => self.conflicts += 1,
            PlanAction::CreateDirectory { .. } => self.create_directories += 1,
            PlanAction::UploadFile { size, .. } => {
                self.upload_files += 1;
                self.upload_bytes += size;
            }
            PlanAction::ReplaceFile { size, .. } => {
                self.replace_files += 1;
                self.upload_bytes += size;
            }
            PlanAction::DeleteFile { .. } => self.delete_files += 1,
            PlanAction::DeleteDirectory { .. } => self.delete_directories += 1,
        }
        self.actions.push(action);
    }
}

pub fn plan_sync(local: &LocalManifest, remote: &RemoteManifest, options: PlanOptions) -> Plan {
    let mut plan = Plan::default();
    plan.warnings.extend(local.warnings.iter().cloned());
    plan.warnings.extend(remote.warnings.iter().cloned());
    for (path, reason) in &remote.conflicts {
        plan.push(PlanAction::Conflict {
            path: path.clone(),
            reason: reason.clone(),
        });
    }

    for (path, local_entry) in &local.entries {
        match remote.entries.get(path) {
            None => {
                if !parent_is_writable(remote, path) {
                    plan.push(PlanAction::Conflict {
                        path: path.clone(),
                        reason: "remote parent folder is not writable".to_string(),
                    });
                } else if local_entry.kind == EntryKind::Directory {
                    plan.push(PlanAction::CreateDirectory { path: path.clone() });
                } else {
                    plan.push(PlanAction::UploadFile {
                        path: path.clone(),
                        size: local_entry.size,
                    });
                }
            }
            Some(remote_entry) if local_entry.kind != remote_entry.kind => {
                plan.push(PlanAction::Conflict {
                    path: path.clone(),
                    reason: format!(
                        "local {:?} conflicts with remote {:?} id {}",
                        local_entry.kind, remote_entry.kind, remote_entry.id
                    ),
                });
            }
            Some(remote_entry) if local_entry.kind == EntryKind::Directory => {
                plan.unchanged += 1;
                if remote_entry.status < 0 {
                    plan.push(PlanAction::Conflict {
                        path: path.clone(),
                        reason: "remote directory is archived".to_string(),
                    });
                }
            }
            Some(remote_entry) => {
                if local_entry.hash.is_some() && local_entry.hash == remote_entry.hash {
                    plan.unchanged += 1;
                } else if !options.overwrite {
                    plan.push(PlanAction::Conflict {
                        path: path.clone(),
                        reason: format!(
                            "remote file {} differs; rerun with --overwrite after atomic replacement is available",
                            remote_entry.id
                        ),
                    });
                } else if remote_entry.status != 0 || !parent_is_writable(remote, path) {
                    plan.push(PlanAction::Conflict {
                        path: path.clone(),
                        reason: "remote file or parent folder is not writable".to_string(),
                    });
                } else {
                    plan.push(PlanAction::ReplaceFile {
                        path: path.clone(),
                        remote_id: remote_entry.id,
                        size: local_entry.size,
                    });
                }
            }
        }
    }

    for (path, remote_entry) in &remote.entries {
        if local.entries.contains_key(path) {
            continue;
        }
        if local.protects(path) || !options.delete {
            plan.retained_remote += 1;
        } else if remote_entry.status != 0 {
            plan.push(PlanAction::Conflict {
                path: path.clone(),
                reason: "remote-only entry is not writable and cannot be deleted".to_string(),
            });
        } else if remote_entry.kind == EntryKind::File {
            plan.push(PlanAction::DeleteFile {
                path: path.clone(),
                remote_id: remote_entry.id,
            });
        } else {
            plan.push(PlanAction::DeleteDirectory {
                path: path.clone(),
                remote_id: remote_entry.id,
            });
        }
    }

    plan.actions.sort_by(compare_actions);
    plan
}

fn parent_is_writable(remote: &RemoteManifest, path: &str) -> bool {
    let parent = parent_path(path);
    if parent.is_empty() {
        remote.root_status == 0
    } else {
        remote
            .entries
            .get(parent)
            .is_none_or(|entry| entry.kind == EntryKind::Directory && entry.status == 0)
    }
}

fn compare_actions(left: &PlanAction, right: &PlanAction) -> Ordering {
    let (left_rank, left_depth, left_path) = action_sort_key(left);
    let (right_rank, right_depth, right_path) = action_sort_key(right);
    left_rank
        .cmp(&right_rank)
        .then_with(|| left_depth.cmp(&right_depth))
        .then_with(|| left_path.cmp(right_path))
}

fn action_sort_key(action: &PlanAction) -> (u8, isize, &str) {
    match action {
        PlanAction::Conflict { path, .. } => (0, path_depth(path) as isize, path),
        PlanAction::CreateDirectory { path } => (1, path_depth(path) as isize, path),
        PlanAction::UploadFile { path, .. } => (2, path_depth(path) as isize, path),
        PlanAction::ReplaceFile { path, .. } => (3, path_depth(path) as isize, path),
        PlanAction::DeleteFile { path, .. } => (4, -(path_depth(path) as isize), path),
        PlanAction::DeleteDirectory { path, .. } => (5, -(path_depth(path) as isize), path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::types::{LocalEntry, RemoteEntry};

    fn local_file(path: &str, hash: u8) -> LocalEntry {
        LocalEntry {
            path: path.to_string(),
            kind: EntryKind::File,
            size: 10,
            hash: Some([hash; 32]),
        }
    }

    fn remote_file(path: &str, id: u32, hash: u8) -> RemoteEntry {
        RemoteEntry {
            path: path.to_string(),
            id,
            parent: 0,
            kind: EntryKind::File,
            size: 10,
            filled: 10,
            hash: Some([hash; 32]),
            status: 0,
        }
    }

    #[test]
    fn equal_hash_is_unchanged() {
        let mut local = LocalManifest::default();
        local.entries.insert("a".into(), local_file("a", 1));
        let mut remote = RemoteManifest::default();
        remote.entries.insert("a".into(), remote_file("a", 7, 1));

        let plan = plan_sync(&local, &remote, PlanOptions::default());
        assert!(plan.actions.is_empty());
        assert_eq!(plan.unchanged, 1);
    }

    #[test]
    fn changed_file_requires_overwrite() {
        let mut local = LocalManifest::default();
        local.entries.insert("a".into(), local_file("a", 1));
        let mut remote = RemoteManifest::default();
        remote.entries.insert("a".into(), remote_file("a", 7, 2));

        let plan = plan_sync(&local, &remote, PlanOptions::default());
        assert!(matches!(plan.actions[0], PlanAction::Conflict { .. }));

        let plan = plan_sync(
            &local,
            &remote,
            PlanOptions {
                overwrite: true,
                ..Default::default()
            },
        );
        assert!(matches!(plan.actions[0], PlanAction::ReplaceFile { .. }));
    }

    #[test]
    fn remote_only_entries_are_deleted_deepest_first() {
        let local = LocalManifest::default();
        let mut remote = RemoteManifest::default();
        remote.entries.insert(
            "old".into(),
            RemoteEntry {
                path: "old".into(),
                id: 1,
                parent: 0,
                kind: EntryKind::Directory,
                size: 0,
                filled: 0,
                hash: None,
                status: 0,
            },
        );
        remote
            .entries
            .insert("old/a".into(), remote_file("old/a", 2, 1));

        let plan = plan_sync(
            &local,
            &remote,
            PlanOptions {
                delete: true,
                ..Default::default()
            },
        );
        assert!(matches!(plan.actions[0], PlanAction::DeleteFile { .. }));
        assert!(matches!(
            plan.actions[1],
            PlanAction::DeleteDirectory { .. }
        ));
    }

    #[test]
    fn protected_local_paths_are_never_mirror_deleted() {
        let mut local = LocalManifest::default();
        local.protected_paths.insert("cache".into());
        let mut remote = RemoteManifest::default();
        remote
            .entries
            .insert("cache/a.bin".into(), remote_file("cache/a.bin", 9, 1));

        let plan = plan_sync(
            &local,
            &remote,
            PlanOptions {
                delete: true,
                overwrite: false,
            },
        );
        assert!(plan.actions.is_empty());
        assert_eq!(plan.retained_remote, 1);
    }

    #[test]
    fn upload_only_plans_are_idempotent_for_small_manifests() {
        let paths = ["a", "b", "c"];
        for local_mask in 0u8..8 {
            for remote_mask in 0u8..8 {
                let mut local = LocalManifest::default();
                let mut remote = RemoteManifest::default();
                for (index, path) in paths.iter().enumerate() {
                    let bit = 1 << index;
                    if local_mask & bit != 0 {
                        local
                            .entries
                            .insert((*path).into(), local_file(path, index as u8));
                    }
                    if remote_mask & bit != 0 {
                        remote
                            .entries
                            .insert((*path).into(), remote_file(path, index as u32, index as u8));
                    }
                }

                let plan = plan_sync(&local, &remote, PlanOptions::default());
                assert!(!plan.has_conflicts());
                assert!(plan.actions.iter().all(|action| matches!(
                    action,
                    PlanAction::UploadFile { .. } | PlanAction::CreateDirectory { .. }
                )));

                for action in &plan.actions {
                    if let PlanAction::UploadFile { path, .. } = action {
                        let local_entry = &local.entries[path];
                        remote.entries.insert(
                            path.clone(),
                            RemoteEntry {
                                path: path.clone(),
                                id: 100,
                                parent: 0,
                                kind: EntryKind::File,
                                size: local_entry.size,
                                filled: local_entry.size,
                                hash: local_entry.hash,
                                status: 0,
                            },
                        );
                    }
                }

                let verification = plan_sync(&local, &remote, PlanOptions::default());
                assert!(verification.actions.is_empty());
            }
        }
    }
}
