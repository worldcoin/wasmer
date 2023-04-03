//! Integration tests for https://github.com/wasmerio/wasmer/issues/3717
use std::sync::Arc;

use bytes::BytesMut;
use virtual_fs::{webc_fs::WebcFileSystem, AsyncWriteExt, DualWriteFile, FileSystem, NullFile};
use wasmer::{Engine, Module};
use wasmer_wasix::{VirtualFile, WasiEnvBuilder};
use webc::v1::{ParseOptions, WebCOwned};

const PYTHON: &[u8] = include_bytes!("../../c-api/examples/assets/python-0.1.0.wasmer");

#[test]
#[tracing::instrument]
fn test_python() {
    init_logging();

    let webc = WebCOwned::parse(PYTHON.into(), &ParseOptions::default()).unwrap();
    let engine = Engine::default();
    let wasm = webc.get_atom(&webc.get_package_name(), "python").unwrap();
    let module = Module::new(&engine, wasm).unwrap();

    let err = WasiEnvBuilder::new("python")
        .args(["-c", "import sys; sys.exit(88);"])
        .fs(Box::new(python_fs(webc)))
        .preopen_dir("/")
        .unwrap()
        .map_dir(".", "/")
        .unwrap()
        .stdout(test_stdout())
        .stderr(test_stdout())
        .run(module)
        .unwrap_err();

    dbg!(&err);

    if let Some(88) = err.as_exit_code().map(|x| x.raw()) {
    } else {
        panic!("{}", err.to_string());
    }
}

fn python_fs(webc: WebCOwned) -> impl FileSystem {
    // Note: the filesystem implementation isn't important here. You could
    // create a memfs and copy all the files over if you want.
    WebcFileSystem::init_all(Arc::new(webc))
}

#[test]
fn php_cgi() {
    init_logging();

    let php_cgi = include_bytes!("php-cgi.wasm");

    let engine = Engine::default();
    let module = Module::new(&engine, php_cgi).unwrap();

    let fs = virtual_fs::mem_fs::FileSystem::default();
    futures::executor::block_on(async {
        fs.new_open_options()
            .create(true)
            .write(true)
            .open("/index.php")
            .unwrap()
            .write_all(include_bytes!("index.php"))
            .await
            .unwrap();
    });

    WasiEnvBuilder::new("php-cgi.wasm")
        .fs(Box::new(fs))
        .preopen_dir("/")
        .unwrap()
        .map_dir(".", "/")
        .unwrap()
        .stdin(Box::new(NullFile::default()))
        .stdout(test_stdout())
        .stderr(test_stdout())
        .run(module)
        .unwrap();
}

/// Get a file object where writes are captured by the test runner.
fn test_stdout() -> Box<dyn VirtualFile + Send + Sync> {
    let mut buffer = BytesMut::new();

    Box::new(DualWriteFile::new(
        Box::new(NullFile::default()),
        move |bytes| {
            buffer.extend_from_slice(bytes);

            // we don't want logs and stdout/stderr to be interleaved, so we add
            // some quick'n'dirty line buffering.
            while let Some(ix) = buffer.iter().position(|&b| b == b'\n') {
                let line = buffer.split_to(ix + 1);
                print!("{}", String::from_utf8_lossy(&line));
            }
        },
    ))
}

fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_test_writer()
        .with_env_filter(
            [
                "info",
                "wasmer_wasix::runners=debug",
                "wasmer_wasix::syscalls=trace",
                "virtual_fs::trace_fs=trace",
            ]
            .join(","),
        )
        .without_time()
        .try_init();
}
