use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};

fn load_root_cert(path: &str) -> RootCertStore {
    let cert = CertificateDer::from_pem_file(path).unwrap();
    let mut certs = RootCertStore::empty();
    certs.add(cert).unwrap();

    certs
}

pub fn configure_client() -> ClientConfig {
    let root_store = load_root_cert("../certs/cert.crt");

    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(_) => {}
        Err(err) => {
            eprintln!("Failed to install CryptoProvider: {:?}", err);
        }
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    client_config
}
