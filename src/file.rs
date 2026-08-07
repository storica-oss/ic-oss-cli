use chrono::prelude::*;
use ic_oss_types::{file::*, format_error};
use serde_bytes::ByteArray;
use sha3::{Digest, Sha3_256};
use tokio::io::AsyncReadExt;
use tokio::{time, time::Duration};

pub const MAX_UPLOAD_RETRIES: u8 = 10;

pub async fn upload_file(
    cli: &ic_oss::bucket::Client,
    enable_hash_index: bool,
    parent: u32,
    file: &str,
    retry: u8,
) -> Result<u32, String> {
    upload_file_with_content_type(cli, enable_hash_index, parent, file, retry, None).await
}

pub async fn upload_file_with_content_type(
    cli: &ic_oss::bucket::Client,
    enable_hash_index: bool,
    parent: u32,
    file: &str,
    retry: u8,
    content_type_override: Option<&str>,
) -> Result<u32, String> {
    if retry > MAX_UPLOAD_RETRIES {
        return Err(format!(
            "retry count {} exceeds maximum {}",
            retry, MAX_UPLOAD_RETRIES
        ));
    }
    let file_path = std::path::Path::new(file);
    let metadata = std::fs::metadata(file_path).map_err(format_error)?;
    if !metadata.is_file() {
        return Err(format!("not a file: {:?}", file));
    }

    let file_size = metadata.len();
    let inferred_content_type = infer::get_from_path(file_path)
        .map_err(format_error)?
        .map(|format| format.mime_type());
    let content_type = if let Some(content_type) = content_type_override {
        let content_type = content_type.trim().to_ascii_lowercase();
        if content_type.is_empty()
            || content_type.len() > 256
            || content_type.chars().any(char::is_control)
        {
            return Err("content type override must contain 1 to 256 safe bytes".into());
        }
        content_type
    } else if let Some(content_type) = inferred_content_type {
        content_type.to_string()
    } else {
        mime_db::lookup(file)
            .unwrap_or("application/octet-stream")
            .to_string()
    };

    let hash: Option<ByteArray<32>> = if enable_hash_index {
        let fs = tokio::fs::File::open(&file_path)
            .await
            .map_err(format_error)?;
        Some(pre_sum_hash(fs).await?.into())
    } else {
        None
    };

    let start_ts: DateTime<Local> = Local::now();
    let input = CreateFileInput {
        parent,
        name: file_path.file_name().unwrap().to_string_lossy().to_string(),
        content_type,
        size: Some(file_size),
        hash,
        ..Default::default()
    };

    let fs = tokio::fs::File::open(&file_path)
        .await
        .map_err(format_error)?;
    let mut res = cli
        .upload(fs, input, move |progress| {
            let ts: DateTime<Local> = Local::now();
            let ts = ts.format("%Y-%m-%d %H:%M:%S").to_string();
            println!(
                "{} uploaded: {:.2}%, {:?}",
                ts,
                upload_percentage(progress.filled, file_size),
                progress
            );
        })
        .await
        .map_err(format_error)?;

    let mut i = 0u8;
    while let Some(err) = res.error {
        i += 1;
        if i > retry {
            return Err(format!("upload failed: {}", err));
        }

        let delay = retry_delay(i);
        println!(
            "upload error: {}.\ntry resumable upload {} after {:?}:",
            err, i, delay
        );
        time::sleep(delay).await;
        let fs = tokio::fs::File::open(&file_path)
            .await
            .map_err(format_error)?;
        res = cli
            .upload_chunks(
                fs,
                res.id,
                Some(file_size),
                None,
                &res.uploaded_chunks,
                move |progress| {
                    let ts: DateTime<Local> = Local::now();
                    let ts = ts.format("%Y-%m-%d %H:%M:%S").to_string();
                    println!(
                        "{} uploaded: {:.2}%, {:?}",
                        ts,
                        upload_percentage(progress.filled, file_size),
                        progress
                    );
                },
            )
            .await;
    }

    println!(
        "upload success, file id: {}, size: {}, chunks: {}, retry: {}, time elapsed: {}",
        res.id,
        res.filled,
        res.uploaded_chunks.len(),
        i,
        Local::now().signed_duration_since(start_ts)
    );
    Ok(res.id)
}

fn upload_percentage(filled: u64, size: u64) -> f32 {
    if size == 0 {
        100.0
    } else {
        (filled as f32 / size as f32) * 100.0
    }
}

fn retry_delay(attempt: u8) -> Duration {
    Duration::from_secs((1u64 << attempt.saturating_sub(1).min(5)).min(30))
}

async fn pre_sum_hash(mut fs: tokio::fs::File) -> Result<[u8; 32], String> {
    let mut hasher = Sha3_256::new();
    let mut buf = vec![0u8; 1024 * 1024 * 2];
    loop {
        let n = fs.read(&mut buf).await.map_err(format_error)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_is_bounded_and_empty_progress_is_complete() {
        assert_eq!(retry_delay(1), Duration::from_secs(1));
        assert_eq!(retry_delay(5), Duration::from_secs(16));
        assert_eq!(retry_delay(10), Duration::from_secs(30));
        assert_eq!(upload_percentage(0, 0), 100.0);
    }
}
