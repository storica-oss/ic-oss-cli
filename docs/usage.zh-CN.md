# 使用 CLI 与 TypeScript 访问 IC OSS

[English](usage.md) · [官方 GitHub](https://github.com/storica-oss) · [CLI 参考](../README.md) · [TypeScript SDK](https://github.com/storica-oss/ic-oss-ts)

IC OSS 将文件和目录保存在 Internet Computer Canister 中。通过 Storica Market 创建 OSS 后，可以使用内置 OSS Admin 进行可视化管理、使用 `ic-oss-cli` 完成同步与备份，或在浏览器/Node.js 应用中集成 `@ldclabs/ic_oss_ts`。

为了兼容已经发布的用户代码，npm 包继续使用 `@ldclabs` 包名。官方源码、Issue 和版本发布迁移到 [`storica-oss`](https://github.com/storica-oss) GitHub 组织。

## 如何选择工具

| 工具                 | 适用场景                                   | 身份认证                         | 安装方式                                     |
| -------------------- | ------------------------------------------ | -------------------------------- | -------------------------------------------- |
| OSS Admin            | 浏览文件、权限、域名与 Cycles              | Internet Identity 或 Plug        | 在 Market 打开 **用户中心 → Open OSS Admin** |
| `ic-oss-cli`         | 目录同步、备份、自动化、健康检查与迁移     | PEM 身份或委托 Access Token      | `cargo install ic-oss-cli`                   |
| `@ldclabs/ic_oss_ts` | Web/Node.js 应用、钱包登录和自定义上传界面 | IC `Identity`、Plug 或委托 Token | `npm install @ldclabs/ic_oss_ts`             |

## 使用前准备

1. **Bucket Canister ID**：从 Market 用户中心复制。每次部署都会生成一个独立的 IC OSS Bucket。
2. **已授权 Principal**：Controller 负责所有权；Manager 可以管理文件；Reader Grant 或委托 Token 可以提供更小范围的权限。
3. **正确网络**：CLI 访问主网必须增加 `--ic`，TypeScript Agent 使用 `https://icp-api.io`。
4. **目录 ID**：`0` 是 Bucket 根目录。

创建 OSS 并不会自动授权你以后使用的每一个身份。CLI PEM、Plug 账户和 Internet Identity 会话可能对应不同 Principal。请先显示完整 Principal，再把这个 Principal 添加为 OSS 管理员。

> Internet Identity Principal 与前端 Origin 相关。通过另一个域名登录可能得到不同 Principal，授权前必须核对完整地址。

## CLI：连接 Market 创建的 OSS

### 1. 安装并创建独立身份

```bash
cargo install ic-oss-cli
ic-oss-cli identity --new --path storica-cli.pem
ic-oss-cli -i storica-cli.pem identity

chmod 600 storica-cli.pem
printf '%s\n' 'storica-cli.pem' >> .gitignore
```

复制命令输出的 Principal，在 Market 用户中心或 OSS Admin 中把它加入 **OSS administrators / Managers**。普通文件上传不需要 Controller 权限。

### 2. 检查 Bucket

```bash
export IC_OSS_BUCKET='aaaaa-aa'

ic-oss-cli -i storica-cli.pem bucket-capabilities \
  --bucket "$IC_OSS_BUCKET" --ic

ic-oss-cli -i storica-cli.pem stat \
  --bucket "$IC_OSS_BUCKET" --kind 2 --ic

ic-oss-cli -i storica-cli.pem ls \
  --bucket "$IC_OSS_BUCKET" --parent 0 --kind 0 --ic
```

`--kind 0` 表示文件，`--kind 1` 表示目录；`stat` 中的其他值表示查看 Bucket 信息。

### 3. 上传和下载文件

```bash
ic-oss-cli -i storica-cli.pem put \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./hello.txt --ic

ic-oss-cli -i storica-cli.pem get \
  --bucket "$IC_OSS_BUCKET" --id 1 --path ./downloads/hello.txt --ic
```

文件 ID 可以从 `put` 的结果或 `ls` 中获取。下载默认使用 SHA3-256 校验。

### 4. 安全同步目录

先查看计划，不写入 Bucket：

```bash
ic-oss-cli -i storica-cli.pem sync \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./site \
  --exclude '.git/**' --exclude '*.tmp' --dry-run --ic
```

确认后上传缺失内容：

```bash
ic-oss-cli -i storica-cli.pem sync \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./site \
  --exclude '.git/**' --exclude '*.tmp' --ic
```

只有审查 dry-run 结果后才应增加 `--overwrite` 或 `--delete`。如果 Bucket 无法证明具备安全删除所需能力，CLI 会拒绝破坏性同步。

### 5. 备份 OSS

```bash
ic-oss-cli -i storica-cli.pem pull \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./oss-backup \
  --dry-run --ic

ic-oss-cli -i storica-cli.pem pull \
  --bucket "$IC_OSS_BUCKET" --parent 0 --path ./oss-backup --ic
```

`pull` 先写入同目录 `.part` 临时文件，并验证远端 revision、generation、长度与哈希后才替换正式文件。

### 使用委托 Access Token

不希望自动化任务持有 Manager PEM 时，可以使用权限范围更小的 COSE Token：

```bash
printf 'base64:%s\n' "$IC_OSS_TOKEN" > access-token.cose
chmod 600 access-token.cose

ic-oss-cli --access-token-file access-token.cose ls \
  --bucket "$IC_OSS_BUCKET" --parent 0 --kind 0 --ic
```

Unix 环境下，CLI 会拒绝权限不安全的 Token 文件，也不会把 Token 写入恢复日志。建议设置较短有效期、精确 Bucket Audience，并仅授予任务所需路径和操作。

## TypeScript：在应用中连接 IC OSS

### 安装

```bash
npm install @ldclabs/ic_oss_ts @dfinity/agent @dfinity/principal
```

### 创建客户端

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

  return BucketCanister.create({ canisterId: bucketId, agent });
}
```

匿名身份只能读取 Bucket 策略明确公开的数据。写入或私有读取必须传入已授权 Identity 或 Access Token。

### 列出目录

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

如果 `page.next` 非空，把它作为下一次调用的 `cursor`，直到返回空值。

### 上传浏览器 File 或 Node.js 文件

```ts
import { Uploader } from "@ldclabs/ic_oss_ts";

const uploader = new Uploader(bucket, 8);
const result = await uploader.upload(
  {
    parent: 0,
    content: file,
    name: file.name,
    contentType: file.type || "application/octet-stream",
  },
  ({ filled, size }) => console.log(`${filled}/${size ?? "?"} bytes`),
);

console.log("file id", result.id);
```

Node.js 可以使用 `content: './report.pdf'`、`name: ''`、`contentType: ''`，SDK 会读取文件名、MIME 与大小。

### 分段读取文件

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

大文件应使用有界 Range 或 Chunk。每次读取都携带 Descriptor 的 `generation`，防止文件被替换时混合两个版本的数据。

### 连接 Plug

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

当前 Plug Principal 必须已被 OSS 授权。白名单中只应请求应用实际访问的 Canister。

### 使用委托 Token

```ts
const bucket = BucketCanister.create({
  canisterId: bucketId,
  agent,
  accessToken: tokenBytes,
});

bucket.setAccessToken(nextTokenBytes);
```

Token 应尽量只保存在内存中。不要把长期 Manager 凭据或无限制 Token 放进公开前端 Bundle。

## 功能对应关系

| 功能             | CLI                                            | TypeScript                                                |
| ---------------- | ---------------------------------------------- | --------------------------------------------------------- |
| Bucket 状态/能力 | `stat`、`bucket-capabilities`、`bucket-health` | `getBucketInfo`、`getStorageMetrics`、`getCapabilities`   |
| 文件/目录列表    | `ls`                                           | `listEntries`、`listFiles`、`listFolders`                 |
| 上传             | `put`、`sync`                                  | `Uploader.upload`、原子上传 Session API                   |
| 下载/备份        | `get`、`pull`                                  | `getFileDescriptor`、`readFileRange`、`readFileChunk`     |
| 创建/移动/删除   | `add`、`mv`、`rm`                              | `ensureFolder`、`moveFile`、`moveFolder`、`deleteEntryIf` |
| 委托访问         | `--access-token-file`                          | `accessToken`、`setAccessToken`                           |
| 运维             | 迁移与垃圾回收命令                             | 迁移、健康检查和垃圾回收 API                              |

## 安全与长期运行清单

- 添加 Manager 或 Controller 前核对完整 Principal。
- Controller 用于所有权和恢复，日常工具使用 Manager 或小权限 Token。
- 不要提交 PEM、钱包密钥或 Bearer Token。
- 使用 `--overwrite` 或 `--delete` 前先运行 `--dry-run`。
- 对关键数据保留带版本的链下备份。
- 监控 Bucket Cycles。购买 OSS 会创建 Canister，但长期存储和计算仍会消耗该 Canister 的 Cycles。
- 更新 WASM 使用 **upgrade**，不要使用会清空应用状态的 **reinstall**。

## 常见问题

| 现象                          | 检查事项                                                                        |
| ----------------------------- | ------------------------------------------------------------------------------- |
| `Unauthorized` 或权限错误     | 对比工具完整 Principal 与 OSS Manager/Reader；II 身份可能因 Origin 不同而变化。 |
| CLI 连错网络                  | 主网增加 `--ic`；自定义本地网络使用全局 `--host <URL>`。                        |
| `sync --delete` 被拒绝        | 运行 `bucket-capabilities`，完成目录存储迁移后再做破坏性同步。                  |
| 换身份或 Token 后无法恢复上传 | 必须使用拥有原 Upload Session 的同一 Token Subject。                            |
| 私有文件无法读取              | 提供已授权 Identity、有效 Reader Grant 或 Audience/Policy 正确的 Token。        |
| Canister 无法接受 Update      | 检查 Cycles，并在达到 freezing threshold 前充值。                               |

完整参数请运行 `ic-oss-cli <command> --help`，TypeScript API 以 npm 包内生成的声明文件为准。
