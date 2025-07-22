use rand::Rng;

/// Returns a thread-local random number generator.
#[inline]
pub fn thread_rng() -> impl Rng {
    #[cfg(feature = "antithesis")]
    return impl_precept::PreceptRng;
    #[cfg(not(feature = "antithesis"))]
    rand::thread_rng()
}

#[cfg(feature = "antithesis")]
mod impl_precept {
    use rand::RngCore;

    pub(crate) struct PreceptRng;

    impl RngCore for PreceptRng {
        #[inline]
        fn next_u32(&mut self) -> u32 {
            self.next_u64() as u32
        }

        #[inline]
        fn next_u64(&mut self) -> u64 {
            precept::dispatch::get_random()
        }

        #[inline]
        fn fill_bytes(&mut self, dst: &mut [u8]) {
            rand_core::impls::fill_bytes_via_next(self, dst)
        }

        #[inline]
        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
            Ok(self.fill_bytes(dest))
        }
    }
}
