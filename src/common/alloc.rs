use std::alloc::{GlobalAlloc, Layout};
use valkey_module::alloc::ValkeyAlloc;

/// The alignment `RedisModule_Alloc` actually guarantees. It has plain `malloc`
/// semantics: 16 bytes on 64-bit platforms (libc malloc on macOS, jemalloc on
/// Linux), regardless of allocation size.
const MALLOC_GUARANTEED_ALIGN: usize = 16;

/// A [`GlobalAlloc`] that fixes over-aligned allocations on top of [`ValkeyAlloc`].
///
/// `ValkeyAlloc` (valkey-module <= 0.1.11) rounds the allocation *size* up to a
/// multiple of `layout.align()` but does nothing to align the returned *pointer*,
/// so any layout with `align > 16` (e.g. crossbeam's `CachePadded`, 128-byte
/// aligned on aarch64, used inside rayon's scheduler) receives misaligned memory.
/// That is undefined behavior; debug builds abort on it at startup via rustc's
/// `slice::from_raw_parts` precondition checks.
///
/// For layouts malloc can satisfy we delegate to `ValkeyAlloc` untouched (keeping
/// valkey's memory accounting). For larger alignments we over-allocate by `align`
/// bytes, round the pointer up, and stash the original pointer in the word just
/// below the returned block so `dealloc` can recover it.
pub struct AlignedValkeyAlloc;

impl AlignedValkeyAlloc {
    #[inline]
    fn padded_layout(layout: Layout) -> Layout {
        // Size + align always leaves room to round up to `align` from a
        // >= 16-byte-aligned base while keeping one pointer-sized slot below the
        // aligned address. Cannot overflow: Layout guarantees size + align - 1
        // does not overflow isize.
        unsafe {
            Layout::from_size_align_unchecked(
                layout.size() + layout.align(),
                MALLOC_GUARANTEED_ALIGN,
            )
        }
    }
}

unsafe impl GlobalAlloc for AlignedValkeyAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.align() <= MALLOC_GUARANTEED_ALIGN {
            return unsafe { ValkeyAlloc.alloc(layout) };
        }

        let raw = unsafe { ValkeyAlloc.alloc(Self::padded_layout(layout)) };
        if raw.is_null() {
            return raw;
        }

        // `raw` is >= 16-byte aligned, so rounding `raw + ptr_size` up to `align`
        // advances by at most `align - ptr_size` and the block of `size` bytes
        // stays inside the `size + align` allocation.
        let addr = raw as usize + size_of::<*mut u8>();
        let aligned = (addr + layout.align() - 1) & !(layout.align() - 1);
        unsafe { (aligned as *mut *mut u8).sub(1).write(raw) };
        aligned as *mut u8
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.align() <= MALLOC_GUARANTEED_ALIGN {
            return unsafe { ValkeyAlloc.alloc_zeroed(layout) };
        }
        let ptr = unsafe { self.alloc(layout) };
        if !ptr.is_null() {
            unsafe { ptr.write_bytes(0, layout.size()) };
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.align() <= MALLOC_GUARANTEED_ALIGN {
            return unsafe { ValkeyAlloc.dealloc(ptr, layout) };
        }
        let raw = unsafe { (ptr as *mut *mut u8).sub(1).read() };
        unsafe { ValkeyAlloc.dealloc(raw, Self::padded_layout(layout)) };
    }
}
