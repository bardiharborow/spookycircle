//! Allocator-free `no_std` program using every allocator-free storage mode.
//! It links with no global allocator, no
//! `alloc` crate, and no `std`; `cargo xtask check-symbols` confirms that no
//! allocator or unwinding symbol made it into the image.
//!
//! The reset handler performs the startup work that static storage requires of
//! the application: copying `.data` (which holds the `const`-initialized
//! `StaticStorage` control state) and zeroing `.bss`, before any safe code
//! touches a queue.

#![no_std]
#![no_main]

use core::{cell::UnsafeCell, hint::black_box, panic::PanicInfo, ptr, ptr::NonNull};

use spookycircle::{BorrowedStorage, Slot, StaticStorage, shared_memory as shm};

/// A queue in a real, immutable `static`: no `static mut`, no allocator.
static QUEUE: StaticStorage<u32, 8> = StaticStorage::new();

/// Caller-owned memory for a shared region (standing in for RAM shared with
/// another core or image).
#[repr(C, align(64))]
struct Region(UnsafeCell<[u8; 512]>);

// SAFETY: accessed only through the shared-region API below, from one
// context, after startup.
unsafe impl Sync for Region {}

static REGION: Region = Region(UnsafeCell::new([0; 512]));

fn run() -> u32 {
    let mut sum = 0;

    // Static storage, split once through safe code.
    let (mut producer, mut consumer) = QUEUE.try_split().unwrap();
    for value in 1..=8 {
        producer.try_push(black_box(value)).unwrap();
    }
    assert!(producer.try_push(9).is_err());
    let mut batch = [0u32; 4];
    sum += consumer.pop_slice(&mut batch) as u32;
    while let Some(value) = consumer.try_pop() {
        sum += value;
    }
    drop(producer);
    assert!(consumer.is_drained());
    drop(consumer);

    // Borrowed slots on the stack, with an exclusive reset between sessions.
    let mut slots = [const { Slot::<u16>::new() }; 3];
    let mut storage = BorrowedStorage::new(&mut slots).unwrap();
    for session in 0..2u16 {
        let (mut producer, mut consumer) = storage.try_split().unwrap();
        producer.try_push(black_box(session)).unwrap();
        if let Some(head) = consumer.peek_mut() {
            *head += 1;
        }
        sum += u32::from(consumer.try_pop().unwrap());
        drop((producer, consumer));
        storage.reset();
    }

    // A shared region in caller-owned memory.
    let base = NonNull::new(REGION.0.get().cast::<u8>()).unwrap();
    let generation = 1;
    // SAFETY: `REGION` is 64-byte aligned, writable, coherent RAM of 512
    // bytes, used by nobody else; the generation is fresh; the endpoints are
    // dropped before this function returns and the region is `'static`.
    unsafe {
        shm::initialize::<4>(base, 512, 16, generation).unwrap();
        let mut producer = shm::attach_producer::<4>(base, 512, 16, generation).unwrap();
        let mut consumer = shm::attach_consumer::<4>(base, 512, 16, generation).unwrap();
        producer.try_push(black_box(7u32).to_le_bytes()).unwrap();
        sum += u32::from_le_bytes(consumer.try_pop().unwrap());
    }
    sum
}

#[panic_handler]
fn panic(_: &PanicInfo<'_>) -> ! {
    loop {
        core::hint::spin_loop();
    }
}

unsafe extern "C" {
    static mut __sbss: u32;
    static mut __ebss: u32;
    static mut __sdata: u32;
    static mut __edata: u32;
    static __sidata: u32;
}

/// Reset handler: initialize RAM, then run.
///
/// # Safety
///
/// Called once by the hardware at reset, before any Rust code.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Reset() -> ! {
    // SAFETY: the linker script defines these symbols as word-aligned
    // bounds of `.bss`, `.data`, and its load image; nothing else runs yet.
    unsafe {
        let mut bss = &raw mut __sbss;
        while bss < &raw mut __ebss {
            ptr::write_volatile(bss, 0);
            bss = bss.add(1);
        }
        let mut data = &raw mut __sdata;
        let mut load = &raw const __sidata;
        while data < &raw mut __edata {
            ptr::write_volatile(data, ptr::read(load));
            data = data.add(1);
            load = load.add(1);
        }
    }
    black_box(run());
    loop {
        core::hint::spin_loop();
    }
}

#[unsafe(link_section = ".vector_table.reset")]
#[used]
static RESET_VECTOR: unsafe extern "C" fn() -> ! = Reset;
