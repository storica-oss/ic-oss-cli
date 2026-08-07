# Use IC OSS with CLI and TypeScript

[简体中文](usage.zh-CN.md) · [Official GitHub](https://github.com/storica-oss) · [CLI reference](../README.md) · [TypeScript SDK](https://github.com/storica-oss/ic-oss-ts)

IC OSS stores files and directories in an Internet Computer canister. After creating an OSS in Storica Market, use the built-in OSS Admin for interactive work, `ic-oss-cli` for repeatable operator workflows, or `@ldclabs/ic_oss_ts` inside a browser or Node.js application.

The npm package keeps its existing `@ldclabs` name for compatibility. The official source, issues, and releases live under the [`storica-oss`](https://github.com/storica-oss) GitHub organization.

## Pick the right tool

| Tool                 | Best for                                                       | Authentication                                 | Install                                     |
| -------------------- | -------------------------------------------------------------- | ---------------------------------------------- | ------------------------------------------- |
| OSS Admin            | Browsing files, permissions, domains, and cycles               | Internet Identity or Plug                      | Open **Account → Open OSS Admin** in Market |
| `ic-oss-cli`         | Directory sync, backup, automation, health, and migration      | PEM identity or delegated access token         | `cargo install ic-oss-cli`                  |
| `@ldclabs/ic_oss_ts` | Web apps, Node.js services, wallet flows, and custom upload UI | IC `Identity`, Plug, or delegated access token | `npm install @ldclabs/ic_oss_ts`            |

## What you need

1. **Bucket canister ID** — copy it from Market **Account**. Each Market deployment is an independent IC OSS bucket.
2. **An authorized Principal** — a controller owns the canister; a Manager can perform normal file administration; a Reader Grant or delegated token can provide narrower access.
3. **The correct network** — use `--ic` with CLI and `https://icp-api.io` with an agent on mainnet.
4. **A folder ID** — folder `0` is the bucket root.

Creating an OSS does not make every identity you use later a Manager. A CLI PEM, Plug account, and Internet Identity session may have different Principals. Display the Principal first, then add that exact Principal as an OSS administrator in Market Account or OSS Admin.

> Internet Identity Principals are scoped by frontend origin. Signing in through another domain can produce a different Principal. Always compare the full Principal before granting access.

## CLI: connect a Market OSS

### 1. Install and create a dedicated identity

```bash
cargo install ic-oss-cli
ic-oss-cli identity --new --path storica-cli.pem
ic-oss-cli -i storica-cli.pem identity
```

Protect the PEM and do not commit it:

```bash
chmod 600 storica-cli.pem
printf '%s\n' 'storica-cli.pem' >> .gitignore
```

Copy the printed Principal, open the OSS in Market **Account**, and add it to **OSS administrators / Managers**. Controller access is not required for ordinary uploads.

### 2. Check the bucket

```bash
export IC_OSS_BUCKET='aaaaa-aa'

ic-oss-cli -i storica-cli.pem bucket-capabilities \
  --bucket "$IC_OSS_BUCKET" --ic

ic-oss-cli -i storica-cli.pem stat \
  --bucket "$IC_OSS_BUCKET" --kind 2 --ic

ic-oss-cli -i storica-cli.pem ls \
  --bucket "$IC_OSS_BUCKET" --parent 0 --kind 0 --ic
```

`--kind 0` means file, `--kind 1` means folder, and any other value in `stat` selects bucket information.

### 3. Upload and download a file

```bash
ic-oss-cli -i storica-cli.pem put \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./hello.txt --ic

ic-oss-cli -i storica-cli.pem get \
  --bucket "$IC_OSS_BUCKET" --id 1 --path ./downloads/hello.txt --ic
```

Use the file ID returned by `put` or listed by `ls`. Downloads verify SHA3-256 by default.

### 4. Safely synchronize a directory

Always inspect a destructive plan first:

```bash
ic-oss-cli -i storica-cli.pem sync \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./site \
  --exclude '.git/**' --exclude '*.tmp' --dry-run --ic
```

Upload missing entries:

```bash
ic-oss-cli -i storica-cli.pem sync \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./site \
  --exclude '.git/**' --exclude '*.tmp' --ic
```

Only add `--overwrite` or `--delete` after reviewing the dry-run output. The CLI refuses destructive synchronization when the bucket cannot prove the required directory and conditional-delete capabilities.

### 5. Back up an OSS

```bash
ic-oss-cli -i storica-cli.pem pull \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./oss-backup \
  --dry-run --ic

ic-oss-cli -i storica-cli.pem pull \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./oss-backup --ic
```

`pull` writes through sibling `.part` files and verifies remote revision, generation, length, and hash before publishing a local file.

### Delegated access token

For automation that should not hold a Manager PEM, place a narrowly scoped COSE token in a protected file:

```bash
printf 'base64:%s\n' "$IC_OSS_TOKEN" > access-token.cose
chmod 600 access-token.cose

ic-oss-cli --access-token-file access-token.cose ls \
  --bucket "$IC_OSS_BUCKET" --parent 0 --kind 0 --ic
```

The CLI rejects unsafe token-file permissions on Unix and never writes the token into its recovery journal. Prefer short expiry, the exact bucket as audience, and only the operations and paths the job needs.

## TypeScript: connect an application

### Install

```bash
npm install @ldclabs/ic_oss_ts @dfinity/agent @dfinity/principal
```

### Create a client

```ts
import { HttpAgent, type Identity } from "@dfinity/agent";
import { Principal } from "@dfinity/principal";
import { BucketCanister } from "@ldclabs/ic_oss_ts";

const bucketId = Principal.fromText("aaaaa-aa");

export async function connectBucket(identity?: Identity) {
  const agent = await HttpAgent.create({
    host: "https://icp-api.io",
    identity,
  });

  return BucketCanister.create({
    canisterId: bucketId,
    agent,
  });
}
```

Anonymous access works only for data exposed by the bucket's public read policy. For writes or private reads, pass an authorized identity or access token.

### List a folder

```ts
const bucket = await connectBucket(identity);
const page = await bucket.listEntries({
  parent: 0,
  cursor: [],
  take: [100],
});

for (const entry of page.entries) {
  console.log(entry.id, entry.name, entry.kind, entry.size[0]);
}
```

Use `page.next` as the next `cursor` until it is empty.

### Upload a browser File or Node.js path

```ts
import { Uploader } from "@ldclabs/ic_oss_ts";

const uploader = new Uploader(bucket, 8);
const result = await uploader.upload(
  {
    parent: 0,
    content: file, // Browser File/Blob, Uint8Array, ReadableStream, or Node.js path
    name: file.name,
    contentType: file.type || "application/octet-stream",
  },
  ({ filled, size }) => console.log(`${filled}/${size ?? "?"} bytes`),
);

console.log("file id", result.id);
```

For a Node.js path, set `content: './report.pdf'`, `name: ''`, and `contentType: ''`; the SDK derives the name, MIME type, and size.

### Read a bounded range

```ts
const descriptor = await bucket.getFileDescriptor(result.id);
const length =
  descriptor.size < 1024n * 1024n ? descriptor.size : 1024n * 1024n;

const firstMiB = await bucket.readFileRange({
  file_id: descriptor.id,
  generation: descriptor.generation,
  offset: 0n,
  length,
});
```

For large files, request bounded ranges or chunks. Keep using the descriptor's `generation`; this prevents a replacement from mixing bytes from two file versions.

### Connect Plug

```ts
import { Principal } from "@dfinity/principal";
import { BucketCanister, createPlugWalletAdapter } from "@ldclabs/ic_oss_ts";

const canisterId = Principal.fromText("aaaaa-aa");
const plug = createPlugWalletAdapter();
const connection = await plug.connect({
  canisters: [canisterId.toText()],
  host: "https://icp-api.io",
  events: {
    accountChanged: () => location.reload(),
    disconnected: () => location.reload(),
  },
});

const bucket = BucketCanister.create({
  canisterId,
  agent: connection.agent(),
});
```

The selected Plug Principal must be authorized by the OSS. Request only the canister IDs the application actually uses.

### Connect with a delegated token

```ts
const bucket = BucketCanister.create({
  canisterId: bucketId,
  agent,
  accessToken: tokenBytes,
});

// Rotate without recreating the client.
bucket.setAccessToken(nextTokenBytes);
```

Keep bearer tokens in memory where possible. Never place long-lived Manager credentials or unrestricted tokens in a public frontend bundle.

## API map

| Task                       | CLI                                            | TypeScript                                                |
| -------------------------- | ---------------------------------------------- | --------------------------------------------------------- |
| Bucket status/capabilities | `stat`, `bucket-capabilities`, `bucket-health` | `getBucketInfo`, `getStorageMetrics`, `getCapabilities`   |
| List files/directories     | `ls`                                           | `listEntries`, `listFiles`, `listFolders`                 |
| Upload                     | `put`, `sync`                                  | `Uploader.upload`, atomic upload session methods          |
| Download/backup            | `get`, `pull`                                  | `getFileDescriptor`, `readFileRange`, `readFileChunk`     |
| Create/move/delete         | `add`, `mv`, `rm`                              | `ensureFolder`, `moveFile`, `moveFolder`, `deleteEntryIf` |
| Delegated access           | `--access-token-file`                          | `accessToken`, `setAccessToken`                           |
| Operations                 | migration and garbage-collection commands      | migration, health, and garbage-collection methods         |

## Security and lifecycle checklist

- Verify the full Principal before adding a Manager or controller.
- Keep controllers for ownership/recovery; use Manager or scoped tokens for routine tools.
- Never commit PEM files, wallet secrets, or bearer tokens.
- Run `sync` and `pull` with `--dry-run` before `--overwrite` or `--delete`.
- Keep a versioned off-canister backup of critical data.
- Monitor bucket cycles. A Market purchase creates the canister, but ongoing storage and computation consume cycles from that canister.
- Use canister **upgrade**, not **reinstall**, when changing WASM; reinstall clears application state.

## Troubleshooting

| Symptom                                              | Check                                                                                                    |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `Unauthorized` or permission error                   | Compare the tool's full Principal with OSS Managers/Readers; II identities may differ by origin.         |
| Local command targets the wrong network              | Add `--ic` for mainnet or pass global `--host <URL>` for a custom local replica.                         |
| `sync --delete` is refused                           | Run `bucket-capabilities` and complete the directory-storage migration before destructive sync.          |
| Upload resumes with another identity/token and fails | Resume with the same token subject that owns the upload session.                                         |
| Private read fails                                   | Supply an authorized identity, active Reader Grant, or valid token with the correct audience and policy. |
| Canister stops accepting updates                     | Check and top up its cycles balance before the freezing threshold is reached.                            |

For every supported option, run `ic-oss-cli <command> --help` or use the generated TypeScript declarations included in the npm package.
