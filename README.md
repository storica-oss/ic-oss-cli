# `ic-oss-cli`

![License](https://img.shields.io/crates/l/ic-oss-cli.svg)
[![Crates.io](https://img.shields.io/crates/d/ic-oss-cli.svg)](https://crates.io/crates/ic-oss-cli)
[![Test](https://github.com/storica-oss/ic-oss-cli/actions/workflows/test.yml/badge.svg)](https://github.com/storica-oss/ic-oss-cli/actions/workflows/test.yml)
[![Docs.rs](https://img.shields.io/docsrs/ic-oss-cli?label=docs.rs)](https://docs.rs/ic-oss-cli)
[![Latest Version](https://img.shields.io/crates/v/ic-oss-cli.svg)](https://crates.io/crates/ic-oss-cli)

A command-line client for [IC OSS](https://github.com/storica-oss), the canister-native object storage service on the Internet Computer. Use it to inspect buckets, upload individual files, safely synchronize directories, create backups, and run bounded maintenance operations.

[Complete tool guide](docs/usage.md) · [中文使用指南](docs/usage.zh-CN.md) · [TypeScript SDK](https://github.com/storica-oss/ic-oss-ts) · [Official GitHub](https://github.com/storica-oss)

## Installation

### Via Cargo

```sh
cargo install ic-oss-cli
# get help info
ic-oss-cli --help
```

### From Source

```sh
git clone https://github.com/storica-oss/ic-oss-cli.git
cd ic-oss-cli
cargo build --release
# get help info
target/release/ic-oss-cli --help
```

## Use with a Storica Market OSS

Every OSS created in Storica Market is an independent Bucket canister. Copy its Canister ID from **Account**, then create a dedicated CLI identity:

```sh
ic-oss-cli identity --new --path storica-cli.pem
ic-oss-cli -i storica-cli.pem identity
chmod 600 storica-cli.pem
```

Add the printed Principal to the deployment's **OSS administrators / Managers** in Market Account or OSS Admin. The CLI Principal is not automatically the same as the Internet Identity or Plug Principal used to create the OSS.

For an IC mainnet bucket, append `--ic` to every bucket command:

```sh
export IC_OSS_BUCKET='aaaaa-aa'

ic-oss-cli -i storica-cli.pem bucket-capabilities \
  --bucket "$IC_OSS_BUCKET" --ic

ic-oss-cli -i storica-cli.pem ls \
  --bucket "$IC_OSS_BUCKET" --parent 0 --kind 0 --ic

ic-oss-cli -i storica-cli.pem put \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./hello.txt --ic
```

Folder `0` is the Bucket root. Keep the PEM outside source control; routine uploads need Manager permission, not canister Controller permission.

## Quick Start

### Identity Management

```sh
# Generate a new identity
ic-oss-cli identity --new --path myid.pem

# Expected output:
# principal: lxph3-nvpsv-yrevd-im4ug-qywcl-5ir34-rpsbs-6olvf-qtugo-iy5ai-jqe
# new identity: myid.pem
```

### File Operations

```sh
# Upload to local canister
ic-oss-cli -i myid.pem put -b mmrxu-fqaaa-aaaap-ahhna-cai --path test.tar.gz

# Upload to mainnet canister
ic-oss-cli -i myid.pem put -b mmrxu-fqaaa-aaaap-ahhna-cai --path test.tar.gz --ic

# Add WASM to cluster
ic-oss-cli -i debug/uploader.pem cluster-add-wasm \
    -c x5573-nqaaa-aaaap-ahopq-cai \
    --path target/wasm32-unknown-unknown/release/ic_oss_bucket.wasm
```

### Personal Hub Transcode Worker

`transcode-run` executes a Hub job with a dedicated worker identity. It requires `ffmpeg` and
`ffprobe`, verifies that the local source size and SHA3-256 match the immutable Hub snapshot,
generates every declared variant, checks the resulting container, H.264 profile/level, dimensions,
AAC stream, and average bitrate, then uploads each output with an explicit content type. The output
Bucket must already be registered in the Hub with the same security class as the source, and both
the worker and Hub principals need the appropriate Bucket write/descriptor permissions.

```sh
ic-oss-cli -i worker.pem transcode-run \
  --hub "$PERSONAL_HUB" \
  --job-id 42 \
  --source ./source.mov \
  --bucket "$PROTECTED_BUCKET" \
  --parent 0 \
  --work-dir ./.transcode/42 \
  --ic
```

Video variants are encoded as H.264/AAC fragmented MP4 with a global SIDX and four-second
fragments. Poster and thumbnail variants support WebP, JPEG, and PNG. The job's `bitrate_bps` is
treated as the maximum accepted average video bitrate; the approval records the value observed by
`ffprobe`.

The work directory is persistent and contains encoded outputs, `worker-journal.json`, and
`owner-approval.json`. Repeating the same command resumes uploaded file IDs and Hub candidates.
The journal contains no PEM or access-token material. A deterministic Hub business rejection
causes the newly uploaded remote file to be deleted; an uncertain/lost response retains the file
ID and journal so a retry can query the Hub before taking any destructive action. Once an
invocation has entered Running, execution errors are also reported back to the Hub as a bounded
Failed result so the job can be retried or cleaned explicitly.

The Owner must use a separate identity and independently inspect the exact output bytes before
Ready promotion. `transcode-approve` rehashes every local output, compares it with the current Hub
candidate, reruns `ffprobe`, compares the new result with the worker report, and only then submits
the Owner approval:

```sh
ic-oss-cli -i owner.pem transcode-approve \
  --hub "$PERSONAL_HUB" \
  --report ./.transcode/42/owner-approval.json \
  --ic
```

Do not run the Owner approval command with the worker identity. For operation on separate machines,
copy the encoded outputs and report together; update report paths only after verifying the
transferred file hashes.

If the Owner cancels a job, or a failed job leaves an output that is no longer its current
candidate, the worker (or Owner) can remove journal-owned remote outputs:

```sh
ic-oss-cli -i worker.pem transcode-cleanup \
  --hub "$PERSONAL_HUB" \
  --job-id 42 \
  --bucket "$PROTECTED_BUCKET" \
  --work-dir ./.transcode/42 \
  --ic
```

Cleanup refuses queued, running, awaiting-verification, and Ready jobs. For a failed job it retains
every file still referenced by a current Hub candidate so the job can be retried; for a cancelled
job it removes all journal-owned outputs. Each deletion is guarded by the recorded SHA3-256 hash,
Bucket revision, and a persistent idempotency request ID. Local encoded files are retained for
audit or a later manual deletion.

### Directory Synchronization

`sync` uploads the contents of the local directory into the selected bucket folder. It is safe by
default: changed remote files and remote-only entries are reported but are not modified unless the
corresponding flag is supplied.

```sh
# Preview the deterministic plan without changing the bucket
ic-oss-cli -i myid.pem sync \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --path ./public \
    --parent 0 \
    --dry-run

# Upload missing folders and files
ic-oss-cli -i myid.pem sync \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --path ./public \
    --parent 0

# Mirror local state after reviewing the dry-run output
ic-oss-cli -i myid.pem sync \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --path ./public \
    --parent 0 \
    --overwrite \
    --delete \
    --exclude '*.tmp' \
    --exclude '.git/**'
```

The local root directory itself is not created; its contents are synchronized into `--parent`.
Symbolic links and non-UTF-8 paths are rejected. Interrupted work is recorded in a recovery journal
outside the synchronized tree and can be resumed by running the same command again. For buckets
with atomic upload support, the journal records non-secret begin request/session IDs and compact
completed-chunk ranges. On restart the CLI queries the canister as the authoritative source, renews
the existing session, and uploads only missing chunks. Access tokens are never journaled, and the
resuming identity or token must resolve to the same subject that owns the session.

When the bucket advertises the required capabilities, the CLI automatically uses a
revision-guarded subtree manifest, batches folders by depth, and batches small files up to 256 KiB.
Each batch contains at most 32 operations and 1.5 MiB of inline file data. Larger files continue to
use resumable atomic upload sessions for both creation and replacement. Missing or expired sessions
are safely replaced with a fresh request ID. Older buckets retain the compatible sequential
scan/upload path but cannot resume the same upload session across CLI processes.

`--overwrite` requires ready directory storage and atomic upload support. `--delete` requires ready
directory storage, conditional deletion, and incremental garbage collection. The CLI refuses these
operations when the bucket cannot prove the required safety properties.

### Directory Download Synchronization

`pull` (alias: `sync-download`) recursively synchronizes a bucket folder into a local directory.
Like upload synchronization, it is non-destructive by default: missing files are downloaded, while
changed local files and local-only entries are left untouched until their respective flags are
supplied.

```sh
# Preview without creating or changing the local directory
ic-oss-cli -i myid.pem pull \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --parent 0 \
    --path ./bucket-backup \
    --dry-run

# Download every missing directory and file
ic-oss-cli -i myid.pem pull \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --parent 0 \
    --path ./bucket-backup

# Mirror the remote folder after reviewing the dry-run plan
ic-oss-cli -i myid.pem sync-download \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --parent 0 \
    --path ./bucket-backup \
    --overwrite \
    --delete \
    --exclude 'cache/**'
```

The selected remote folder itself is not created; its contents are written directly below
`--path`. The destination may be absent and is created only during a non-dry-run execution.
`--overwrite` atomically replaces changed regular files. `--delete` removes local-only regular
files followed by empty directories, deepest first. Excluded paths, symbolic links, and directories
containing protected symbolic links are never mirror-deleted.

Each file is downloaded into a sibling `.part` file. The CLI validates chunk ordering and length,
checks SHA3-256 when the bucket exposes a content hash, rechecks the remote file revision and
generation, flushes the temporary file, and only then renames it into place. A failed or interrupted
download therefore does not publish a partial destination file. If an older bucket does not expose
file hashes, an existing file remains a conflict unless `--overwrite` is supplied.

For mainnet, append `--ic`. Delegated token authentication works the same way as upload sync:

```sh
ic-oss-cli --access-token-file access-token.cose pull \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --path ./bucket-backup \
    --parent 0 \
    --ic
```

### Access-token Authentication

Bucket commands can authenticate an anonymous or non-Manager identity with a delegated bearer
token. Pass the token through a protected file instead of placing the secret directly on the command
line. The file may contain raw COSE bytes or UTF-8 text prefixed with `base64:`.

```sh
printf 'base64:%s\n' "$IC_OSS_TOKEN" > access-token.cose
chmod 600 access-token.cose

ic-oss-cli --access-token-file access-token.cose sync \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --path ./public \
    --parent 0 \
    --ic
```

On Unix, the CLI rejects token files readable or writable by group/other users. Empty files,
symbolic links, non-regular files, invalid base64, and files larger than 64 KiB are also rejected.
The token is held in memory for the command and is never copied into the synchronization recovery
journal or printed by the CLI. The token policies must grant the file/folder list and write
operations required by the selected synchronization mode.

The file is re-read before every authenticated request, including each resumable upload chunk. To
rotate a token without exposing a partially written credential, write and protect a sibling file,
then rename it atomically over the active path:

```sh
printf 'base64:%s\n' "$NEW_IC_OSS_TOKEN" > access-token.cose.new
chmod 600 access-token.cose.new
mv access-token.cose.new access-token.cose
```

An active upload session remains bound to the token subject. A replacement token used to resume the
session must therefore carry the same subject and a valid audience/policy, even when its signature
or trusted signing key has rotated. When rotating the signing key, first trust both the old and new
public keys, atomically replace the token file, and only then retire the old key. This overlap lets
an already in-flight request finish while all subsequent requests reload the new credential.

### Reader Grant administration

Reader Grant administration uses the PEM caller identity; it does not consume `--access-token-file`.
Setting or clearing the authority requires the Bucket controller. Grant mutation calls must be sent
by that configured authority and use a stable 1–64 byte hexadecimal request ID for safe retries:

```sh
ic-oss-cli -i controller.pem reader-authority \
  --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
  --authority "$PERSONAL_HUB"

ic-oss-cli -i hub-authority.pem reader-grant-upsert \
  --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
  --subject "$MEMBER" \
  --expires-at-ms 1798761600000 \
  --version 1 \
  --request-id 0123456789abcdef

ic-oss-cli -i member.pem reader-grant-self \
  --bucket mmrxu-fqaaa-aaaap-ahhna-cai

ic-oss-cli -i hub-authority.pem reader-grant-revoke \
  --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
  --subject "$MEMBER" \
  --version 2 \
  --request-id fedcba9876543210

# Deliberately clearing the authority always requires the explicit flag.
ic-oss-cli -i controller.pem reader-authority \
  --bucket mmrxu-fqaaa-aaaap-ahhna-cai --clear
```

Reuse the same request ID only when retrying the identical operation. A different payload with the
same request ID is rejected rather than silently replacing the original result.

### Bucket Upgrade and Directory Migration

After upgrading a legacy bucket to a release with directory storage v2, run the migration with the
canister controller identity. Each update call is bounded; `--until-complete` submits additional
calls until the state becomes `Ready`.

```sh
# Inspect the advertised API and current migration state
ic-oss-cli -i controller.pem bucket-capabilities \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai

# Process one bounded batch (1 to 1000 entries)
ic-oss-cli -i controller.pem bucket-migrate-directory \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --max-items 1000

# Or continue automatically until verification activates v2 storage
ic-oss-cli -i controller.pem bucket-migrate-directory \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --max-items 1000 \
    --until-complete

# Verify directory indexes, upload sessions, and garbage collection backlog
ic-oss-cli -i controller.pem bucket-health \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai
```

Do not enable destructive synchronization until capabilities report `migration_state = Ready` and
directory health reports no duplicate names, dangling entries, or migration error. If migration
reports `Failed`, inspect `bucket-health`, repair the reported legacy data, clear the failed state,
then resume migration:

```sh
ic-oss-cli -i controller.pem bucket-retry-directory-migration \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai
ic-oss-cli -i controller.pem bucket-migrate-directory \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --until-complete
```

### Garbage Collection Operations

File deletion is logical and immediate; physical chunk removal is incremental. A canister timer
normally drains the queue, while a Manager can run bounded collection manually when
`bucket-health` reports a backlog. Collection also reaps expired upload sessions.

```sh
# Process one bounded batch (1 to 1024 chunk slots)
ic-oss-cli -i manager.pem bucket-collect-garbage \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --max-chunks 1024

# Continue until both pending items and chunks reach zero
ic-oss-cli -i manager.pem bucket-collect-garbage \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai \
    --max-chunks 1024 \
    --until-clean

ic-oss-cli -i manager.pem bucket-health \
    --bucket mmrxu-fqaaa-aaaap-ahhna-cai
```

`--until-clean` still uses bounded canister calls; it stops with an error if a backlog remains but a
call processes no chunk slots.

Add `--ic` to any bucket command when operating on the Internet Computer mainnet. For a local
replica on a non-default address, pass the global option `--host <URL>` before the subcommand.

## Documentation

For detailed usage instructions:

```sh
ic-oss-cli --help
ic-oss-cli identity --help
ic-oss-cli upload --help
ic-oss-cli sync --help
ic-oss-cli bucket-migrate-directory --help
ic-oss-cli bucket-collect-garbage --help
```

## License

Copyright © 2024-2025 [LDC Labs](https://github.com/ldclabs).

Licensed under the MIT License. See [LICENSE](LICENSE-MIT) for details.
