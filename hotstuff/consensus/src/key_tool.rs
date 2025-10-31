use std::env;
use codec::Encode;
use w3f_bls::{DoublePublicKeyScheme, Keypair, SerializableToBytes, TinyBLS381};
use sp_core::{ByteArray, Pair};

fn main() {
    let args = env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        panic!("invalid params");
    }
    match args[1].as_str() {
        "bls381" => generate_bls_381(),
        _ => panic!("invalid mission"),
    }
}

fn generate_bls_381() {
    use sp_application_crypto::bls381;
    let (pair, seed) = bls381::AppPair::generate();
    // let secret = w3f_bls::SecretKey::<TinyBLS381>::from_seed(seed.as_slice());
    // let keypair = Keypair {
    //     public: secret.into_public(),
    //     secret,
    // };

    println!("seed:\n0x{}", hex::encode(seed.as_slice()));
    // println!("secret:\n0x{}", hex::encode(keypair.secret.to_bytes()));
    println!("pubkey:\n0x{}", hex::encode(pair.public().as_slice()));
    // let double_pk = DoublePublicKeyScheme::into_double_public_key(&keypair).to_bytes();
    // println!("double_pubkey:\n0x{}", hex::encode(double_pk));
}
