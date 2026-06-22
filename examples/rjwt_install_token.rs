use umask::Mode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::env;
    use std::str::FromStr;
    use std::time::{Duration, SystemTime};

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use base64::engine::general_purpose::STANDARD_NO_PAD;
    use pathlink::Link;
    use rjwt::{AlgKind, SigningKey};
    use tc_ir::Claim;
    use umask::{USER_EXEC, USER_WRITE};

    use tinychain::auth::{Actor, Token};

    let mut host: Option<String> = None;
    let mut actor_id: Option<String> = None;
    let mut libs: Vec<String> = Vec::new();
    let mut txn_id: Option<String> = None;
    let mut secret_key_b64: Option<String> = None;
    let mut alg = AlgKind::Falcon512;
    let mut ttl_secs: u64 = 3600;
    let mut mode = USER_WRITE;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => host = args.next(),
            "--actor" => actor_id = args.next(),
            "--lib" => {
                if let Some(value) = args.next() {
                    libs.push(value);
                } else {
                    return Err("missing --lib value".into());
                }
            }
            "--txn-id" => txn_id = args.next(),
            "--secret-key-b64" => secret_key_b64 = args.next(),
            "--alg" => alg = parse_alg(args.next().ok_or("missing --alg value")?.as_str())?,
            "--ttl-secs" => {
                ttl_secs = args.next().ok_or("missing --ttl-secs value")?.parse()?;
            }
            "--mode" => mode = parse_mode(args.next().ok_or("missing --mode value")?.as_str())?,
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    let host = host.ok_or("missing --host")?;
    let actor_id = actor_id.ok_or("missing --actor")?;
    if libs.is_empty() {
        return Err("missing --lib".into());
    }

    let host = Link::from_str(&host)?;
    let mut claims = Vec::new();
    for lib in &libs {
        claims.push(Claim::new(Link::from_str(lib)?, mode));
    }

    let signing_key = if let Some(secret_key_b64) = secret_key_b64.as_ref() {
        let key_bytes = STANDARD
            .decode(secret_key_b64.trim())
            .or_else(|_| STANDARD_NO_PAD.decode(secret_key_b64.trim()))?;
        SigningKey::from_bytes(alg, &key_bytes)?
    } else {
        SigningKey::generate_falcon512()?
    };
    let secret_key_b64 = secret_key_b64.unwrap_or_else(|| STANDARD.encode(signing_key.to_bytes()));

    let actor = Actor::with_signing_key(actor_id.clone(), signing_key);

    let token = Token::new(
        host.clone(),
        SystemTime::now(),
        Duration::from_secs(ttl_secs),
        actor.id().clone(),
        claims.first().cloned().ok_or("missing --lib claim")?,
    );
    let mut signed = actor.sign_token(token)?;
    for claim in claims.iter().skip(1).cloned() {
        signed = actor.consume_and_sign(signed, host.clone(), claim, SystemTime::now())?;
    }
    if let Some(txn_id) = txn_id {
        let txn_claim = Claim::new(
            Link::from_str(&format!("/txn/{txn_id}"))?,
            USER_EXEC | USER_WRITE,
        );
        signed = actor.consume_and_sign(signed, host.clone(), txn_claim, SystemTime::now())?;
    }

    let public_key_b64 = STANDARD.encode(actor.verifying_key().to_bytes());

    println!("host: {host}");
    for claim in &claims {
        println!("claim: {}", claim.link);
    }
    println!("actor_id: {actor_id}");
    println!("public_key_b64: {public_key_b64}");
    println!("secret_key_b64: {secret_key_b64}");
    println!("bearer_token: {}", signed.into_jwt());

    Ok(())
}

fn parse_alg(alg: &str) -> Result<rjwt::AlgKind, Box<dyn std::error::Error>> {
    match alg.trim().to_ascii_lowercase().as_str() {
        "falcon512" | "falcon-512" | "fn-dsa-512" => Ok(rjwt::AlgKind::Falcon512),
        "ed25519" | "eddsa" => Ok(rjwt::AlgKind::Ed25519),
        other => Err(format!("unsupported signature algorithm: {other}").into()),
    }
}

fn print_usage() {
    eprintln!(
        "Usage: rjwt_install_token --host <http(s)://host[:port]> --actor <id> --lib <path> [--lib <path> ...] [--txn-id <id>] [--secret-key-b64 <b64>] [--alg falcon512|ed25519] [--ttl-secs <n>] [--mode <octal>]\n\
         Example:\n\
          cargo run --example rjwt_install_token -- \\\n\
             --host http://127.0.0.1:8702 --actor example-admin \\\n\
             --lib /lib/example-devco/a/0.1.0 --lib /lib/example-devco/example/0.1.0\n"
    );
}

fn parse_mode(mode: &str) -> Result<Mode, Box<dyn std::error::Error>> {
    let digits = mode.trim().strip_prefix("0o").unwrap_or(mode.trim());
    if digits.is_empty() || !digits.chars().all(|c| ('0'..='7').contains(&c)) {
        return Err(format!("invalid octal mode: {mode}").into());
    }
    Ok(Mode::from(u32::from_str_radix(digits, 8)?))
}
