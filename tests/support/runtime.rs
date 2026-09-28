/// Drop every runtime task before a restart test reopens its storage owners.
/// Native debug futures use the standalone Collection/Chain test stack bound.
pub(crate) fn run<F, T>(make: impl FnOnce() -> F + Send + 'static) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    use futures::FutureExt;

    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            let (send, receive) = std::sync::mpsc::sync_channel(0);
            runtime.spawn(async move {
                let result = std::panic::AssertUnwindSafe(make()).catch_unwind().await;
                send.send(result).unwrap();
            });
            match receive.recv().unwrap() {
                Ok(result) => result,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        })
        .unwrap()
        .join()
        .unwrap()
}
