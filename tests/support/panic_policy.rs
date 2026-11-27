//! Built with `cargo build`, never used to infer application policy from a test harness.
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use freqfs::{Cache, FileSave};
use get_size::GetSize;

struct Entry(usize);

impl GetSize for Entry {
    fn get_size(&self) -> usize {
        self.0
    }
}

impl FileSave for Entry {
    async fn save(&self, _file: &mut tokio::fs::File) -> io::Result<u64> {
        panic!("panic-policy: detached eviction worker");
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    if !cfg!(panic = "abort") {
        return Err(io::Error::other("probe requires panic=abort"));
    }

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("direct") => panic!("panic-policy: direct"),
        Some("eviction") => {
            // The parent owns this directory because abort does not run destructors.
            let path = PathBuf::from(args.next().expect("parent-owned directory"));
            let cache = Cache::<Entry>::new(128, Some(2), 0, Duration::from_secs(3));
            let root = cache.load(path)?;
            let first = root
                .write()
                .await
                .create_file("first".into(), Entry(96))
                .await?;

            // Capacity pressure invokes FileSave on the cache's real detached worker.
            let _second = root
                .write()
                .await
                .create_file("second".into(), Entry(64))
                .await?;

            drop(first);
            panic!("eviction did not invoke the injected failure");
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected direct or eviction",
        )),
    }
}
