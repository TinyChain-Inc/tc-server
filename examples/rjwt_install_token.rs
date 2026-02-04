fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::env;
    use std::str::FromStr;
    use std::time::{Duration, SystemTime};

    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use pathlink::Link;
    use tc_ir::Claim;
    use tc_value::Value;
    use umask::USER_WRITE;

    use tinychain::auth::{Actor, Token};

    let mut host: Option<String> = None;
    let mut actor_id: Option<String> = None;
    let mut libs: Vec<String> = Vec::new();
    let mut ttl_secs: u64 = 3600;

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
            "--ttl-secs" => {
                ttl_secs = args
                    .next()
                    .ok_or("missing --ttl-secs value")?
                    .parse()?;
            }
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
        claims.push(Claim::new(Link::from_str(lib)?, USER_WRITE));
    }
    let actor = Actor::new(Value::from(actor_id.clone()));
    let token = Token::new(
        host.clone(),
        SystemTime::now(),
        Duration::from_secs(ttl_secs),
        actor.id().clone(),
        claims
            .first()
            .cloned()
            .ok_or("missing --lib claim")?,
    );
    let mut signed = actor.sign_token(token)?;
    for claim in claims.iter().skip(1).cloned() {
        signed = actor.consume_and_sign(signed, host.clone(), claim, SystemTime::now())?;
    }

    let public_key_b64 = STANDARD.encode(actor.public_key().to_bytes());

    println!("host: {host}");
    for claim in &claims {
        println!("claim: {}", claim.link);
    }
    println!("actor_id: {actor_id}");
    println!("public_key_b64: {public_key_b64}");
    println!("bearer_token: {}", signed.into_jwt());

    Ok(())
}

fn print_usage() {
    eprintln!(
        "Usage: rjwt_install_token --host <http://host:port> --actor <id> --lib <path> [--lib <path> ...] [--ttl-secs <n>]\n\
         Example:\n\
          cargo run --example rjwt_install_token -- \\\n\
             --host http://127.0.0.1:8702 --actor example-admin \\\n\
             --lib /lib/example-devco/a/0.1.0 --lib /lib/example-devco/example/0.1.0\n"
    );
}
