//! Two processes sharing one queue through a file mapped with
//! `mmap(MAP_SHARED)` (`shared-memory` feature, Unix).
//!
//! ```text
//! cargo run --release --features shared-memory --example shared_memory
//! ```
//!
//! The parent creates and sizes the region file, maps it, initializes the
//! queue, and only then starts the child, passing the path, configuration,
//! and generation on the command line: that is the synchronized startup
//! handoff the `unsafe` attach contract requires. The child maps the same
//! file (at a different address) and attaches as consumer; the parent
//! attaches as producer. Neither side allocates inside the queue; `std` is
//! used only for the file, the process, and printing.
//!
//! Records are `[u8; 16]`: a little-endian sequence number and a checksum
//! that the application encodes and decodes itself.

#[cfg(unix)]
fn main() {
    unix::main();
}

#[cfg(not(unix))]
fn main() {
    eprintln!("this example needs a Unix `mmap`");
}

#[cfg(unix)]
mod unix {
    use std::{
        env,
        fs::{self, OpenOptions},
        os::fd::AsRawFd,
        path::Path,
        process::Command,
        ptr::NonNull,
        thread,
    };

    use spookycircle::shared_memory as shm;

    const RECORD: usize = 16;
    const CAPACITY: usize = 256;
    const COUNT: u64 = 1_000_000;

    /// A `MAP_SHARED` mapping, unmapped on drop.
    struct Mapping {
        base: NonNull<u8>,
        len: usize,
    }

    impl Mapping {
        fn open(path: &Path, len: usize) -> Self {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            // SAFETY: a new shared read-write mapping of `len` bytes of an
            // open file that is at least that long.
            let map = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };
            assert_ne!(map, libc::MAP_FAILED, "mmap failed");
            Self {
                base: NonNull::new(map.cast()).unwrap(),
                len,
            }
        }
    }

    impl Drop for Mapping {
        fn drop(&mut self) {
            // SAFETY: mapped in `open`; the endpoints borrowing it are gone.
            unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
        }
    }

    fn encode(sequence: u64) -> [u8; RECORD] {
        let mut record = [0; RECORD];
        record[..8].copy_from_slice(&sequence.to_le_bytes());
        record[8..].copy_from_slice(&(!sequence).to_le_bytes());
        record
    }

    fn decode(record: [u8; RECORD]) -> u64 {
        let sequence = u64::from_le_bytes(record[..8].try_into().unwrap());
        let check = u64::from_le_bytes(record[8..].try_into().unwrap());
        assert_eq!(check, !sequence, "corrupt record");
        sequence
    }

    pub fn main() {
        match env::args().nth(1).as_deref() {
            Some("consumer") => consumer(),
            _ => producer(),
        }
    }

    fn producer() {
        let layout = shm::layout::<RECORD>(CAPACITY).unwrap();
        let path = env::temp_dir().join(format!("spookycircle-example-{}", std::process::id()));
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap()
            .set_len(layout.size() as u64)
            .unwrap();
        let mapping = Mapping::open(&path, layout.size());
        let generation = u64::from(std::process::id());

        // SAFETY: the file was just created by this process and nobody else
        // knows about it yet, so access is exclusive; the mapping is
        // coherent, writable, and page-aligned.
        unsafe { shm::initialize::<RECORD>(mapping.base, mapping.len, CAPACITY, generation) }
            .unwrap();

        // Startup handoff: the child learns everything from its arguments,
        // after initialization completed.
        let mut child = Command::new(env::current_exe().unwrap())
            .args([
                "consumer",
                path.to_str().unwrap(),
                &layout.size().to_string(),
                &generation.to_string(),
            ])
            .spawn()
            .unwrap();

        // SAFETY: initialized above; the mapping outlives the endpoint, and
        // this process claims the producer role once.
        let mut producer = unsafe {
            shm::attach_producer::<RECORD>(mapping.base, mapping.len, CAPACITY, generation)
        }
        .unwrap();
        println!("producer: base {:p}", mapping.base);
        for sequence in 0..COUNT {
            let mut record = encode(sequence);
            // Back-pressure policy (not part of the wait-free operation).
            while let Err(full) = producer.try_push(record) {
                record = full.into_inner();
                thread::yield_now();
            }
        }
        // Closing the role after the last publication lets the consumer
        // detect the end of the stream definitively.
        drop(producer);

        let status = child.wait().unwrap();
        drop(mapping);
        fs::remove_file(&path).unwrap();
        assert!(status.success(), "consumer failed");
        println!("producer: sent {COUNT} records");
    }

    fn consumer() {
        let args: Vec<String> = env::args().collect();
        let (path, len, generation) = (
            Path::new(&args[2]),
            args[3].parse::<usize>().unwrap(),
            args[4].parse::<u64>().unwrap(),
        );
        let mapping = Mapping::open(path, len);
        // SAFETY: the producer initialized the region before starting this
        // process (the startup handoff), the mapping outlives the endpoint,
        // and this process claims the consumer role once.
        let mut consumer = unsafe {
            shm::attach_consumer::<RECORD>(mapping.base, mapping.len, CAPACITY, generation)
        }
        .unwrap();
        println!("consumer: base {:p}", mapping.base);
        let mut expected = 0u64;
        let mut batch = [[0; RECORD]; 32];
        loop {
            let n = consumer.pop_slice(&mut batch);
            for record in &batch[..n] {
                assert_eq!(decode(*record), expected, "gap or duplicate");
                expected += 1;
            }
            if n == 0 {
                if consumer.is_drained() {
                    break;
                }
                thread::yield_now();
            }
        }
        println!("consumer: received {expected} records in order");
        assert_eq!(expected, COUNT);
    }
}
