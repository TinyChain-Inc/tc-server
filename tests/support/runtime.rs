/// Native collection futures need the same bounded debug-test stack as the
/// standalone Collection and Chain suites. No task outlives this test runtime.
pub(crate) fn run<F: std::future::Future<Output = ()> + Send + 'static>(make: fn() -> F) {
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
            if let Err(panic) = receive.recv().unwrap() {
                std::panic::resume_unwind(panic);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
