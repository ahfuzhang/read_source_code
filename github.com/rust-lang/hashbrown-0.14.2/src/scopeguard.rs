// Extracted from the scopeguard crate
use core::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    ptr,
};

pub struct ScopeGuard<T, F>
where
    F: FnMut(&mut T),  //??? 看不懂这个是要干啥
{
    dropfn: F,  //一个闭包
    value: T,   //值，会传入RawTableInner对象
}

#[inline] //构造对象
pub fn guard<T, F>(value: T, dropfn: F) -> ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    ScopeGuard { dropfn, value }
}

impl<T, F> ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    #[inline]
    pub fn into_inner(guard: Self) -> T {
        // Cannot move out of Drop-implementing types, so
        // ptr::read the value out of a ManuallyDrop<Self>
        // Don't use mem::forget as that might invalidate value
        let guard = ManuallyDrop::new(guard);
        unsafe {
            let value = ptr::read(&guard.value);
            // read the closure so that it is dropped
            let _ = ptr::read(&guard.dropfn);
            value
        }
    }
}

impl<T, F> Deref for ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.value  //转移所有权???
    }
}

impl<T, F> DerefMut for ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T, F> Drop for ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    #[inline]
    fn drop(&mut self) {
        (self.dropfn)(&mut self.value);  //执行闭包函数
    }
}
