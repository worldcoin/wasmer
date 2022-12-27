use std::{
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::Future;
use tokio::sync::mpsc;
use tracing::*;
use wasmer::{FunctionEnvMut, Instance, Memory, Module, Store};
use wasmer_vbus::{
    BusSpawnedProcess, SpawnOptions, SpawnOptionsConfig, VirtualBusError, VirtualBusInvokable,
    VirtualBusProcess, VirtualBusScope, VirtualBusSpawner,
};
use wasmer_wasi_types::wasi::{Errno, ExitCode};

use super::{BinFactory, BinaryPackage, ModuleCache};
use crate::{
    import_object_for_all_wasi_versions, runtime::SpawnType, SpawnedMemory, WasiEnv, WasiError,
    WasiFunctionEnv, WasiRuntimeImplementation,
};

pub fn spawn_exec(
    binary: BinaryPackage,
    name: &str,
    store: Store,
    config: SpawnOptionsConfig<WasiEnv>,
    runtime: &Arc<dyn WasiRuntimeImplementation + Send + Sync + 'static>,
    compiled_modules: &ModuleCache,
) -> wasmer_vbus::Result<BusSpawnedProcess> {
    // Load the module
    #[cfg(feature = "sys")]
    let compiler = store.engine().name();
    #[cfg(not(feature = "sys"))]
    let compiler = "generic";
    let module = compiled_modules.get_compiled_module(&store, binary.hash().as_str(), compiler);
    let module = match (module, binary.entry.as_ref()) {
        (Some(a), _) => a,
        (None, Some(entry)) => {
            let module = Module::new(&store, &entry[..]).map_err(|err| {
                error!(
                    "failed to compile module [{}, len={}] - {}",
                    name,
                    entry.len(),
                    err
                );
                VirtualBusError::CompileError
            });
            if module.is_err() {
                config.env.cleanup(Some(Errno::Noexec as ExitCode));
            }
            let module = module?;
            compiled_modules.set_compiled_module(binary.hash().as_str(), compiler, &module);
            module
        }
        (None, None) => {
            error!("package has no entry [{}]", name,);
            config.env.cleanup(Some(Errno::Noexec as ExitCode));
            return Err(VirtualBusError::CompileError);
        }
    };

    // If the file system has not already been union'ed then do so
    config.env().state.fs.conditional_union(&binary);

    // Now run the module
    spawn_exec_module(module, store, config, runtime)
}

pub fn spawn_exec_module(
    module: Module,
    store: Store,
    config: SpawnOptionsConfig<WasiEnv>,
    runtime: &Arc<dyn WasiRuntimeImplementation + Send + Sync + 'static>,
) -> wasmer_vbus::Result<BusSpawnedProcess> {
    // Create a new task manager
    let tasks = runtime.new_task_manager();

    // Create the signaler
    let pid = config.env().pid();
    let signaler = Box::new(config.env().process.clone());

    // Now run the binary
    let (exit_code_tx, exit_code_rx) = mpsc::unbounded_channel();
    {
        // Determine if shared memory needs to be created and imported
        let shared_memory = module.imports().memories().next().map(|a| *a.ty());

        // Determine if we are going to create memory and import it or just rely on self creation of memory
        let memory_spawn = match shared_memory {
            Some(ty) => {
                #[cfg(feature = "sys")]
                let style = store.tunables().memory_style(&ty);
                SpawnType::CreateWithType(SpawnedMemory {
                    ty,
                    #[cfg(feature = "sys")]
                    style,
                })
            }
            None => SpawnType::Create,
        };

        // Create a thread that will run this process
        let runtime = runtime.clone();
        let tasks_outer = tasks.clone();
        tasks_outer
            .task_wasm(
                Box::new(move |mut store, module, memory| {
                    // Create the WasiFunctionEnv
                    let mut wasi_env = config.env().clone();
                    wasi_env.runtime = runtime;
                    wasi_env.tasks = tasks;
                    let mut wasi_env = WasiFunctionEnv::new(&mut store, wasi_env);

                    // Let's instantiate the module with the imports.
                    let (mut import_object, init) =
                        import_object_for_all_wasi_versions(&module, &mut store, &wasi_env.env);
                    if let Some(memory) = memory {
                        import_object.define(
                            "env",
                            "memory",
                            Memory::new_from_existing(&mut store, memory),
                        );
                    }
                    let instance = match Instance::new(&mut store, &module, &import_object) {
                        Ok(a) => a,
                        Err(err) => {
                            error!("wasi[{}]::wasm instantiate error ({})", pid, err);
                            wasi_env
                                .data(&store)
                                .cleanup(Some(Errno::Noexec as ExitCode));
                            return;
                        }
                    };

                    init(&instance, &store).unwrap();

                    // Initialize the WASI environment
                    if let Err(err) = wasi_env.initialize(&mut store, &instance) {
                        error!("wasi[{}]::wasi initialize error ({})", pid, err);
                        wasi_env
                            .data(&store)
                            .cleanup(Some(Errno::Noexec as ExitCode));
                        return;
                    }

                    // If this module exports an _initialize function, run that first.
                    if let Ok(initialize) = instance.exports.get_function("_initialize") {
                        if let Err(e) = initialize.call(&mut store, &[]) {
                            let code = match e.downcast::<WasiError>() {
                                Ok(WasiError::Exit(code)) => code as ExitCode,
                                Ok(WasiError::UnknownWasiVersion) => {
                                    debug!("wasi[{}]::exec-failed: unknown wasi version", pid);
                                    Errno::Noexec as ExitCode
                                }
                                Err(err) => {
                                    debug!("wasi[{}]::exec-failed: runtime error - {}", pid, err);
                                    Errno::Noexec as ExitCode
                                }
                            };
                            let _ = exit_code_tx.send(code);
                            wasi_env
                                .data(&store)
                                .cleanup(Some(Errno::Noexec as ExitCode));
                            return;
                        }
                    }

                    // Let's call the `_start` function, which is our `main` function in Rust.
                    let start = instance.exports.get_function("_start").ok();

                    // If there is a start function
                    debug!("wasi[{}]::called main()", pid);
                    let ret = if let Some(start) = start {
                        match start.call(&mut store, &[]) {
                            Ok(_) => 0,
                            Err(e) => match e.downcast::<WasiError>() {
                                Ok(WasiError::Exit(code)) => code,
                                Ok(WasiError::UnknownWasiVersion) => {
                                    debug!("wasi[{}]::exec-failed: unknown wasi version", pid);
                                    Errno::Noexec as u32
                                }
                                Err(err) => {
                                    debug!("wasi[{}]::exec-failed: runtime error - {}", pid, err);
                                    9999u32
                                }
                            },
                        }
                    } else {
                        debug!("wasi[{}]::exec-failed: missing _start function", pid);
                        Errno::Noexec as u32
                    };
                    debug!("wasi[{}]::main() has exited with {}", pid, ret);

                    // Cleanup the environment
                    wasi_env.data(&store).cleanup(Some(ret));

                    // Send the result
                    let _ = exit_code_tx.send(ret);
                    drop(exit_code_tx);
                }),
                store,
                module,
                memory_spawn,
            )
            .map_err(|err| {
                error!("wasi[{}]::failed to launch module - {}", pid, err);
                VirtualBusError::UnknownError
            })?
    };

    let inst = Box::new(SpawnedProcess {
        exit_code: Mutex::new(None),
        exit_code_rx: Mutex::new(exit_code_rx),
    });
    Ok(BusSpawnedProcess {
        inst,
        stdin: None,
        stdout: None,
        stderr: None,
        signaler: Some(signaler),
    })
}

impl BinFactory {
    pub fn try_built_in(
        &self,
        name: String,
        parent_ctx: Option<&FunctionEnvMut<'_, WasiEnv>>,
        store: &mut Option<Store>,
        builder: &mut Option<SpawnOptions<WasiEnv>>,
    ) -> wasmer_vbus::Result<BusSpawnedProcess> {
        // We check for built in commands
        if let Some(parent_ctx) = parent_ctx {
            if self.commands.exists(name.as_str()) {
                return self
                    .commands
                    .exec(parent_ctx, name.as_str(), store, builder);
            }
        } else {
            if self.commands.exists(name.as_str()) {
                tracing::warn!("builtin command without a parent ctx - {}", name);
            }
        }
        Err(VirtualBusError::NotFound)
    }
}

impl VirtualBusSpawner<WasiEnv> for BinFactory {
    fn spawn<'a>(
        &'a self,
        name: String,
        store: Store,
        config: SpawnOptionsConfig<WasiEnv>,
        _fallback: Box<dyn VirtualBusSpawner<WasiEnv>>,
    ) -> Pin<Box<dyn Future<Output = wasmer_vbus::Result<BusSpawnedProcess>> + 'a>> {
        Box::pin(async move {
            if config.remote_instance().is_some() {
                config.env.cleanup(Some(Errno::Inval as ExitCode));
                return Err(VirtualBusError::Unsupported);
            }

            // Find the binary (or die trying) and make the spawn type
            let binary = self
                .get_binary(name.as_str())
                .await
                .ok_or(VirtualBusError::NotFound);
            if binary.is_err() {
                config.env.cleanup(Some(Errno::Noent as ExitCode));
            }
            let binary = binary?;

            // Execute
            spawn_exec(
                binary,
                name.as_str(),
                store,
                config,
                &self.runtime,
                &self.cache,
            )
        })
    }
}

#[derive(Debug)]
pub(crate) struct SpawnedProcess {
    pub exit_code: Mutex<Option<ExitCode>>,
    pub exit_code_rx: Mutex<mpsc::UnboundedReceiver<ExitCode>>,
}

impl VirtualBusProcess for SpawnedProcess {
    fn exit_code(&self) -> Option<ExitCode> {
        let mut exit_code = self.exit_code.lock().unwrap();
        if let Some(exit_code) = exit_code.as_ref() {
            return Some(exit_code.clone());
        }
        let mut rx = self.exit_code_rx.lock().unwrap();
        match rx.try_recv() {
            Ok(code) => {
                exit_code.replace(code);
                Some(code)
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                let code = Errno::Canceled as ExitCode;
                exit_code.replace(code);
                Some(code)
            }
            _ => None,
        }
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        {
            let exit_code = self.exit_code.lock().unwrap();
            if exit_code.is_some() {
                return Poll::Ready(());
            }
        }
        let mut rx = self.exit_code_rx.lock().unwrap();
        let mut rx = Pin::new(rx.deref_mut());
        match rx.poll_recv(cx) {
            Poll::Ready(code) => {
                let code = code.unwrap_or(Errno::Canceled as ExitCode);
                {
                    let mut exit_code = self.exit_code.lock().unwrap();
                    exit_code.replace(code);
                }
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl VirtualBusScope for SpawnedProcess {
    fn poll_finished(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        VirtualBusProcess::poll_ready(self, cx)
    }
}

impl VirtualBusInvokable for SpawnedProcess {}