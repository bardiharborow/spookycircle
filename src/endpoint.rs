//! The public method surface shared by every endpoint family.
//!
//! Heap-owned, borrowed, and shared-region endpoints must expose identical
//! producer and consumer methods. These macros
//! generate them, with their documentation, for a wrapper type whose `raw`
//! field is a raw endpoint (which pads itself to its own cache line).
//! Everything forwards to the single
//! generic implementation in `raw`, so no backend adds code to the data path.

/// Generates the producer methods and `Debug` for a wrapper type.
///
/// `impl[...]` takes the impl generics, `elem` the element type, and
/// `copy_where` the bound under which `push_slice` exists.
macro_rules! producer_api {
    (
        impl[$($gen:tt)*] $ty:ty,
        elem = $elem:ty,
        copy_where = [$($copy:tt)*],
        name = $name:literal $(,)?
    ) => {
        impl<$($gen)*> $ty {
            /// Attempts to insert `value` at the back of the queue.
            ///
            /// # Ownership
            ///
            /// On success the queue takes ownership of `value`; it will later
            /// be returned by exactly one consumer operation or, for typed
            /// queues, dropped once during final cleanup. On failure the
            /// untouched value is handed back inside [`Full`](crate::Full);
            /// the library never clones or drops it.
            ///
            /// # Semantics
            ///
            /// Returns `Err(Full)` only after a fresh acquire load of the
            /// consumer's position confirms that all `capacity` slots are
            /// occupied. A full result is never based solely on stale cached
            /// state.
            ///
            /// This method does not consult the consumer's liveness. After the
            /// consumer has been dropped, pushes continue to succeed until the
            /// queue is full, but no such value will ever be consumed.
            ///
            /// # Progress
            ///
            /// Wait-free: a fixed sequence of at most one acquire load, one
            /// slot write, and one release store. No allocation, retry loop,
            /// or user code runs inside this call. Retrying `try_push` in a
            /// loop until it succeeds is a caller policy and is not wait-free
            /// as a whole.
            ///
            /// # Errors
            ///
            /// Returns [`Full`](crate::Full), holding `value`, when all
            /// `capacity` slots are occupied.
            #[inline]
            pub fn try_push(&mut self, value: $elem) -> Result<(), crate::Full<$elem>> {
                self.raw.try_push(value)
            }

            /// Returns the fixed logical capacity, exactly as requested at
            /// creation.
            #[inline]
            pub fn capacity(&self) -> usize {
                self.raw.capacity()
            }

            /// Returns the number of queued elements observed at one instant.
            ///
            /// The result is a snapshot: the consumer may remove elements
            /// immediately after this call returns, so it may be stale by the
            /// time you read it. It is never a reservation; use the result of
            /// [`try_push`](Self::try_push) as the authority for that
            /// operation.
            #[inline]
            pub fn len(&self) -> usize {
                self.raw.len()
            }

            /// Returns `capacity() - len()` for a single snapshot.
            ///
            /// The result may be stale immediately and is not a reservation.
            #[inline]
            pub fn remaining_capacity(&self) -> usize {
                self.capacity().saturating_sub(self.len())
            }

            /// Returns whether the queue held no elements at one instant.
            ///
            /// The result may be stale immediately.
            #[inline]
            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }

            /// Returns whether the queue held `capacity()` elements at one
            /// instant.
            ///
            /// The result may be stale immediately; a subsequent `try_push`
            /// may still succeed or fail regardless of this answer.
            #[inline]
            pub fn is_full(&self) -> bool {
                self.len() == self.capacity()
            }

            /// Returns whether the consumer role had not yet been closed.
            ///
            /// The flag is monotonic: once this returns `false` it never
            /// returns `true` again. A `true` result is only a snapshot, and
            /// the consumer may be dropped immediately afterwards. It is not
            /// an acknowledgment that any queued value will be consumed.
            #[inline]
            pub fn is_consumer_alive(&self) -> bool {
                self.raw.is_consumer_alive()
            }
        }

        impl<$($gen)*> $ty where $($copy)* {
            /// Copies the longest prefix of `source` that fits and returns its
            /// length.
            ///
            /// The copied prefix is published with a single release store, so
            /// the consumer observes either none or all of it. Elements keep
            /// their order. Returns 0 without publishing anything when
            /// `source` is empty or the queue is full.
            ///
            /// # Ownership
            ///
            /// The element type is `Copy`, so `source` is only read; the queue
            /// holds copies of the transferred prefix. The result may be less
            /// than `source.len()`; the caller decides what to do with the
            /// untransferred suffix.
            ///
            /// # Progress
            ///
            /// Bounded by `min(source.len(), capacity())` own steps, with at
            /// most one acquire load of the consumer's position. It does not
            /// wait for the consumer.
            #[inline]
            pub fn push_slice(&mut self, source: &[$elem]) -> usize {
                self.raw.push_slice(source)
            }
        }

        /// Reports the role, capacity, a length snapshot, and a
        /// consumer-liveness snapshot. Queued elements are never inspected or
        /// formatted.
        ///
        /// The fields are observed one after another, not atomically as a
        /// set, so a concurrently running consumer can make them mutually
        /// inconsistent.
        impl<$($gen)*> core::fmt::Debug for $ty {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.debug_struct($name)
                    .field("capacity", &self.capacity())
                    .field("len", &self.len())
                    .field("consumer_alive", &self.is_consumer_alive())
                    .finish()
            }
        }
    };
}

/// Generates the consumer methods and `Debug` for a wrapper type; see
/// [`producer_api!`].
macro_rules! consumer_api {
    (
        impl[$($gen:tt)*] $ty:ty,
        elem = $elem:ty,
        copy_where = [$($copy:tt)*],
        name = $name:literal $(,)?
    ) => {
        impl<$($gen)*> $ty {
            /// Removes and returns the oldest element, or `None` if the queue
            /// is empty.
            ///
            /// # Ownership
            ///
            /// On success the caller receives full ownership of the value; the
            /// library never clones or drops it. On `None` nothing changes
            /// hands.
            ///
            /// # Semantics
            ///
            /// Returns `None` only after a fresh acquire load of the
            /// producer's position confirms that no element is published. An
            /// empty result is never based solely on stale cached state.
            /// Producer disconnection is not reported here; see
            /// [`is_drained`](Self::is_drained).
            ///
            /// # Progress
            ///
            /// Wait-free: at most one acquire load, one slot read, and one
            /// release store. No allocation, retry loop, or user code runs
            /// inside this call. Retrying `try_pop` in a loop is a caller
            /// policy and is not wait-free as a whole.
            #[inline]
            pub fn try_pop(&mut self) -> Option<$elem> {
                self.raw.try_pop()
            }

            /// Moves the oldest element into `destination` and returns a
            /// reference to it there, or returns `None` if the queue is
            /// empty.
            ///
            /// This is [`try_pop`](Self::try_pop) with the value written
            /// straight to caller-chosen memory. `try_pop` must copy the
            /// element out of its slot before releasing the slot and can only
            /// hand it to the caller afterwards, so an element too large to
            /// return in registers is copied through a temporary. Here the
            /// element is copied once, from the slot into `destination`.
            /// That only matters for large elements whose destination is
            /// memory (a buffer, a `Vec`'s spare capacity); otherwise prefer
            /// `try_pop`.
            ///
            /// # Ownership
            ///
            /// On success the element is moved into `destination` and the
            /// returned reference points to it. The caller now owns it:
            /// because `destination` is a
            /// [`MaybeUninit`](core::mem::MaybeUninit), it is never dropped
            /// automatically, so a type with a destructor leaks unless the
            /// caller takes it out (for example with
            /// [`MaybeUninit::assume_init_read`](core::mem::MaybeUninit::assume_init_read))
            /// or drops it in place. Whatever `destination` held before is
            /// overwritten without being dropped. On `None`, `destination`
            /// is left untouched and nothing changes hands.
            ///
            /// # Semantics and progress
            ///
            /// As for `try_pop`: `None` only after a fresh acquire load of the
            /// producer's position confirms that the queue is empty, and
            /// wait-free, with at most one acquire load, one slot copy, and
            /// one release store. No user code, including a destructor, runs
            /// inside this call.
            ///
            /// ```
            /// use core::mem::MaybeUninit;
            /// use spookycircle::StaticStorage;
            ///
            /// static STORAGE: StaticStorage<[u64; 8], 2> = StaticStorage::new();
            /// let (mut producer, mut consumer) = STORAGE.try_split().unwrap();
            /// producer.try_push([7; 8]).unwrap();
            ///
            /// let mut batch: Vec<[u64; 8]> = Vec::with_capacity(4);
            /// while batch.len() < batch.capacity() {
            ///     let len = batch.len();
            ///     if consumer.try_pop_into(&mut batch.spare_capacity_mut()[0]).is_none() {
            ///         break;
            ///     }
            ///     // SAFETY: the element at `len` was just initialized.
            ///     unsafe { batch.set_len(len + 1) };
            /// }
            /// assert_eq!(batch, [[7; 8]]);
            ///
            /// let mut slot = MaybeUninit::uninit();
            /// assert_eq!(consumer.try_pop_into(&mut slot), None);
            /// ```
            #[inline]
            pub fn try_pop_into<'d>(
                &mut self,
                destination: &'d mut core::mem::MaybeUninit<$elem>,
            ) -> Option<&'d mut $elem> {
                self.raw.try_pop_into(destination)
            }

            /// Borrows the oldest element without removing it.
            ///
            /// Uses the same empty-confirmation rule as
            /// [`try_pop`](Self::try_pop) and never advances the queue. The
            /// reference borrows the consumer mutably, so no operation that
            /// could remove the element can run while it is live; the producer
            /// cannot overwrite the slot because it has not been released.
            ///
            /// # Ownership
            ///
            /// No ownership changes hands, on success or on `None`: the
            /// element stays owned by the queue and is still returned by a
            /// later `try_pop` (or, for typed queues, dropped at final
            /// cleanup). Nothing is cloned.
            ///
            /// Wait-free, as for `try_pop`.
            #[inline]
            pub fn peek(&mut self) -> Option<&$elem> {
                self.raw.peek()
            }

            /// Mutably borrows the oldest element without removing it.
            ///
            /// Any mutation made through the reference is what a later
            /// [`try_pop`](Self::try_pop) returns or what final cleanup drops.
            /// If user code panics while holding the reference, the element
            /// stays queued with whatever mutation had been completed.
            ///
            /// # Ownership
            ///
            /// No ownership changes hands, on success or on `None`: the
            /// element stays owned by the queue. Replacing it through the
            /// reference (for example with `mem::replace`) hands the old value
            /// to the caller and leaves the new one queued in its place.
            ///
            /// Wait-free, as for `try_pop`.
            #[inline]
            pub fn peek_mut(&mut self) -> Option<&mut $elem> {
                self.raw.peek_mut()
            }

            /// Returns the fixed logical capacity, exactly as requested at
            /// creation.
            #[inline]
            pub fn capacity(&self) -> usize {
                self.raw.capacity()
            }

            /// Returns the number of queued elements observed at one instant.
            ///
            /// The result is a snapshot: the producer may add elements
            /// immediately after this call returns, so it may be stale by the
            /// time you read it. Use the result of [`try_pop`](Self::try_pop)
            /// as the authority for that operation.
            #[inline]
            pub fn len(&self) -> usize {
                self.raw.len()
            }

            /// Returns `capacity() - len()` for a single snapshot.
            ///
            /// The result may be stale immediately.
            #[inline]
            pub fn remaining_capacity(&self) -> usize {
                self.capacity().saturating_sub(self.len())
            }

            /// Returns whether the queue held no elements at one instant.
            ///
            /// The result may be stale immediately. For a definitive
            /// end-of-stream test use [`is_drained`](Self::is_drained).
            #[inline]
            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }

            /// Returns whether the queue held `capacity()` elements at one
            /// instant.
            ///
            /// The result may be stale immediately.
            #[inline]
            pub fn is_full(&self) -> bool {
                self.len() == self.capacity()
            }

            /// Returns whether the producer role had not yet been closed.
            ///
            /// The flag is monotonic: once this returns `false` it never
            /// returns `true` again. A `true` result is only a snapshot, and
            /// the producer may be dropped immediately afterwards. A `false`
            /// result does not mean the queue is empty; published elements
            /// remain available.
            #[inline]
            pub fn is_producer_alive(&self) -> bool {
                self.raw.is_producer_alive()
            }

            /// Returns `true` once the producer has been dropped and every
            /// value it published has been consumed.
            ///
            /// This is definitive, not a snapshot: the producer publishes its
            /// liveness change only after its last publication, so observing
            /// "producer gone" followed by "nothing queued" proves that no
            /// value will ever arrive. Once this returns `true` it stays
            /// `true`.
            ///
            /// A `false` result carries no such guarantee and may be stale
            /// immediately.
            ///
            /// Wait-free: two acquire loads.
            #[inline]
            pub fn is_drained(&self) -> bool {
                self.raw.is_drained()
            }
        }

        impl<$($gen)*> $ty where $($copy)* {
            /// Copies the oldest available elements into a prefix of
            /// `destination` and returns how many were copied.
            ///
            /// At most `destination.len()` elements are removed, in FIFO
            /// order; the remainder of `destination` is left unchanged. All
            /// removed slots are released with a single release store, so the
            /// producer observes either none or all of the batch. Returns 0
            /// without releasing anything when `destination` is empty or the
            /// queue is empty.
            ///
            /// # Ownership
            ///
            /// The element type is `Copy`, so overwriting `destination` runs
            /// no destructor and the removed elements are simply forgotten by
            /// the queue.
            ///
            /// # Progress
            ///
            /// Bounded by `min(destination.len(), capacity())` own steps, with
            /// at most one acquire load of the producer's position. It does
            /// not wait for the producer.
            #[inline]
            pub fn pop_slice(&mut self, destination: &mut [$elem]) -> usize {
                self.raw.pop_slice(destination)
            }
        }

        /// Reports the role, capacity, a length snapshot, and a
        /// producer-liveness snapshot. Queued elements are never inspected or
        /// formatted.
        ///
        /// The fields are observed one after another, not atomically as a
        /// set, so a concurrently running producer can make them mutually
        /// inconsistent.
        impl<$($gen)*> core::fmt::Debug for $ty {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.debug_struct($name)
                    .field("capacity", &self.capacity())
                    .field("len", &self.len())
                    .field("producer_alive", &self.is_producer_alive())
                    .finish()
            }
        }
    };
}
