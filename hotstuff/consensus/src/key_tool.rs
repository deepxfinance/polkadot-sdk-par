use std::env;
use sp_core::{ByteArray, Pair};
use sp_core::crypto::{ExposeSecret, SecretStringError, SecretUri};

fn main() {
    let args = env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        panic!("invalid params");
    }
    match args[1].as_str() {
        "bls381" => if args.len() == 2 {
            generate_bls_381();
        } else {
            from_string_seed(args);
        },
        _ => panic!("invalid mission"),
    }
}


fn generate_bls_381() {
    use sp_application_crypto::bls381;
    let (pair, seed) = bls381::AppPair::generate();
    println!("seed:\n0x{}", hex::encode(seed.as_slice()));
    println!("pubkey:\n0x{}", hex::encode(pair.public().as_slice()));
}

fn from_string_seed(args: Vec<String>) {
    use hotstuff_primitives::bls_crypto::Pair as BLS381Pair;
    use std::str::FromStr;

    let s = args[2].as_str();
    let mut password_override = None;
    if args.len() >= 4 {
        password_override = Some(args[3].as_str());
    }

    let SecretUri { junctions, phrase, password } = SecretUri::from_str(s).unwrap();
    let password =
        password_override.or_else(|| password.as_ref().map(|p| p.expose_secret().as_str()));

    let (root, seed) = if let Some(stripped) = phrase.expose_secret().strip_prefix("0x") {
        array_bytes::hex2bytes(stripped)
            .ok()
            .and_then(|seed_vec| {
                let mut seed = <hotstuff_primitives::bls_crypto::Pair as Pair>::Seed::default();
                if seed.as_ref().len() == seed_vec.len() {
                    seed.as_mut().copy_from_slice(&seed_vec);
                    Some((BLS381Pair::from_seed(&seed), seed))
                } else {
                    None
                }
            })
            .ok_or(SecretStringError::InvalidSeed)
            .unwrap()
    } else {
        BLS381Pair::from_phrase(phrase.expose_secret().as_str(), password)
            .map_err(|_| SecretStringError::InvalidPhrase)
            .expect("invalid phrase, if need derive, please add \"//\" as prefix.")
    };
    let (pair, seed) = root.derive(junctions.into_iter(), Some(seed))
        .map_err(|_| SecretStringError::InvalidPath).unwrap();

    println!("seed:\n0x{}", hex::encode(seed.unwrap().as_slice()));
    println!("pubkey:\n0x{}", hex::encode(pair.public().as_slice()));
}
