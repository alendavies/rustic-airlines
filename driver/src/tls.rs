use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};

use std::env;
use std::path::Path;

fn load_root_cert(path: &Path) -> RootCertStore {
    let cert = CertificateDer::from_pem_file(path).unwrap();
    let mut certs = RootCertStore::empty();
    certs.add(cert).unwrap();

    certs
}

pub fn configure_client() -> ClientConfig {
    let project_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let path = Path::new(&project_dir)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("certs")
        .join("cert.crt");
    let root_store = load_root_cert(&path);

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
