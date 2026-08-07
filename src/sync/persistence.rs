use std::{
    env,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

pub fn user_cache_dir() -> PathBuf {
    env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("LOCALAPPDATA").map(PathBuf::from))
        .or_else(|| {
            env::var_os("HOME").map(|home| {
                let home = PathBuf::from(home);
                if cfg!(target_os = "macos") {
                    home.join("Library/Caches")
                } else {
                    home.join(".cache")
                }
            })
        })
        .unwrap_or_else(|| env::temp_dir().join("ic-oss-user-cache"))
        .join("ic-oss")
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("invalid persistence path {:?}", path))?;
    fs::create_dir_all(parent)
        .map_err(|err| format!("failed to create cache directory {:?}: {}", parent, err))?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = File::create(&temporary)
        .map_err(|err| format!("failed to create temporary file {:?}: {}", temporary, err))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|err| format!("failed to write temporary file {:?}: {}", temporary, err))?;
    fs::rename(&temporary, path)
        .map_err(|err| format!("failed to replace persisted file {:?}: {}", path, err))
}
