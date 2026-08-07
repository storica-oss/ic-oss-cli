use anyhow::{bail, Context, Result};
use candid::Principal;
use clap::Parser;
use ic_oss_types::{
    cose::{cose_sign1, cose_sign1_to_vec, EdDSA, Token, BUCKET_TOKEN_AAD},
    permission::Policies,
};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_EXPIRES_IN: u64 = 365 * 24 * 60 * 60;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

#[derive(Debug, Parser)]
#[command(about = "Issue a short-lived local IC-OSS COSE/CWT access token")]
struct Args {
    /// Principal represented by the delegated token.
    #[arg(long)]
    subject: String,

    /// Bucket canister principal that will accept the token.
    #[arg(long)]
    audience: String,

    /// Raw COSE_Sign1 token output path.
    #[arg(long)]
    token: PathBuf,

    /// PKCS#8 Ed25519 signing key. Created securely when absent and reused when present.
    #[arg(long)]
    key: PathBuf,

    /// Token lifetime in seconds (60 seconds to 365 days).
    #[arg(long, default_value_t = 3_600)]
    expires_in: u64,

    /// IC-OSS permission policies. `*` grants all permissions and is intended only for local tests.
    #[arg(long, default_value = "*")]
    policies: String,
}

fn secure_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options
}

fn write_secure_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = secure_options()
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", path.display()))?;
    Ok(())
}

fn write_secure_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("token path must have a UTF-8 file name")?;
    let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    write_secure_new(&temporary, bytes)?;
    fs::rename(&temporary, path)
        .with_context(|| format!("replace {} atomically", path.display()))?;
    Ok(())
}

fn load_or_create_key(path: &Path) -> Result<Ed25519KeyPair> {
    if path.exists() {
        let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
        return Ed25519KeyPair::from_pkcs8(&bytes)
            .map_err(|_| anyhow::anyhow!("{} is not a valid Ed25519 PKCS#8 key", path.display()));
    }

    let generated = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow::anyhow!("failed to generate Ed25519 signing key"))?;
    write_secure_new(path, generated.as_ref())?;
    Ed25519KeyPair::from_pkcs8(generated.as_ref())
        .map_err(|_| anyhow::anyhow!("generated Ed25519 key could not be loaded"))
}

fn candid_blob(bytes: &[u8]) -> String {
    bytes.iter().map(|value| format!("\\{value:02x}")).collect()
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !(60..=MAX_EXPIRES_IN).contains(&args.expires_in) {
        bail!("--expires-in must be between 60 and {MAX_EXPIRES_IN} seconds (365 days)");
    }

    let subject = Principal::from_text(&args.subject).context("invalid --subject principal")?;
    let audience = Principal::from_text(&args.audience).context("invalid --audience principal")?;
    let policies = Policies::try_from(args.policies.as_str())
        .map_err(|error| anyhow::anyhow!("invalid --policies: {error}"))?;
    let parent = args
        .token
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    if let Some(key_parent) = args
        .key
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(key_parent)
            .with_context(|| format!("create {}", key_parent.display()))?;
    }

    let key = load_or_create_key(&args.key)?;
    let public_key = key.public_key().as_ref();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs() as i64;
    let token_claims = Token {
        subject,
        audience,
        policies: policies.to_string(),
    };
    let claims = token_claims.clone().to_cwt(now, args.expires_in as i64);
    let mut sign1 = cose_sign1(claims, EdDSA, None)
        .map_err(|error| anyhow::anyhow!("create COSE_Sign1: {error}"))?;
    let signing_input = sign1
        .prepare_signature(None, None, Some(BUCKET_TOKEN_AAD))
        .context("prepare COSE signature")?;
    sign1
        .set_signature(key.sign(&signing_input).as_ref().to_vec())
        .context("set COSE signature")?;
    let encoded = cose_sign1_to_vec(&sign1).context("encode COSE token")?;
    let public_key_array: [u8; 32] = public_key
        .try_into()
        .context("Ed25519 public key must contain 32 bytes")?;
    let verified = Token::from_sign1(
        &encoded,
        &[],
        &[public_key_array.into()],
        BUCKET_TOKEN_AAD,
        now,
    )
    .map_err(|error| anyhow::anyhow!("verify generated COSE token: {error}"))?;
    if verified != token_claims {
        bail!("generated COSE token did not round-trip to the requested claims");
    }
    write_secure_atomic(&args.token, &encoded)?;

    let public_key_path = args.token.with_extension("pub.hex");
    fs::write(&public_key_path, hex::encode(public_key))
        .with_context(|| format!("write {}", public_key_path.display()))?;
    let trust_argument_path = args.token.with_extension("trust.did");
    fs::write(
        &trust_argument_path,
        format!(
            "(record {{ trusted_eddsa_pub_keys = opt vec {{ blob \"{}\" }} }})\n",
            candid_blob(public_key)
        ),
    )
    .with_context(|| format!("write {}", trust_argument_path.display()))?;

    println!("token: {}", args.token.display());
    println!("signing key: {}", args.key.display());
    println!("public key: {}", public_key_path.display());
    println!("trust argument: {}", trust_argument_path.display());
    println!("subject: {subject}");
    println!("audience: {audience}");
    println!("policies: {policies}");
    println!("expires in: {} seconds", args.expires_in);
    Ok(())
}
