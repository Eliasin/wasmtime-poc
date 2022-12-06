use anyhow::{anyhow, Context};
use cargo_toml::{Manifest, Value};

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

fn wait_for_path_deletion<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
    let path = path.as_ref();
    for _ in 1..32 {
        if path.exists() {
            std::thread::sleep(Duration::from_millis(500));
        } else {
            return Ok(());
        }
    }

    return Err(anyhow!(
        "Timed out waiting for {} to be deleted",
        path.display()
    ));
}

fn wait_for_path_creation<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
    let path = path.as_ref();
    for _ in 1..32 {
        if !path.exists() {
            std::thread::sleep(Duration::from_millis(500));
        } else {
            return Ok(());
        }
    }

    return Err(anyhow!(
        "Timed out waiting for {} to be created",
        path.display()
    ));
}

const MODULE_BUILD_DIR: &str = "../../module-build/";
const MODULE_DIR: &str = "../../modules/";

fn build_modules() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../modules/");
    let module_build_dir_path = Path::new(MODULE_BUILD_DIR);
    if module_build_dir_path.exists() {
        println!("cargo:warning=Deleting old module build directory...");
        fs::remove_dir_all(
            &module_build_dir_path
                .canonicalize()
                .context("Failed to canonicalize module directory path")?,
        )?;
        wait_for_path_deletion(module_build_dir_path)?;
        fs::create_dir(module_build_dir_path)?;
        wait_for_path_creation(module_build_dir_path)?;
    } else {
        fs::create_dir(module_build_dir_path)?;
        wait_for_path_creation(module_build_dir_path)?;
    }

    let mut module_build_threads = vec![];

    println!("cargo:warning=********** Starting Module Build Process **********");
    for module_dir in fs::read_dir(
        Path::new(MODULE_DIR)
            .canonicalize()
            .context("Failed to canonicalize module directory path")?,
    )? {
        let module_dir = match module_dir {
            Ok(module_dir) => module_dir,
            Err(e) => {
                println!("cargo:warning=Error traversing modules directory: {e:?}");
                continue;
            }
        };

        if !module_dir.path().is_dir() {
            continue;
        }

        let cargo_path = module_dir.path().join("Cargo.toml");
        if !cargo_path.exists() || !cargo_path.is_file() {
            println!(
                "cargo:warning=> Skipping {} because no 'Cargo.toml' found",
                module_dir.path().display()
            );
            continue;
        }

        println!(
            "cargo:warning=> Discovered module at {}",
            module_dir.path().display()
        );

        let canonical_module_path = module_dir.path().canonicalize().with_context(|| {
            format!(
                "Failed canonicalizing module path: {}",
                module_dir.path().display()
            )
        })?;

        let mut cargo_build_command = {
            let mut c = Command::new("cargo");
            c.args(["build", "--target", "wasm32-unknown-unknown"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .current_dir(&canonical_module_path);
            c
        };

        let command_thread_handle = match cargo_build_command.spawn() {
            Ok(command_thread_handle) => command_thread_handle,
            Err(e) => {
                println!(
                    "cargo:warning=Error starting cargo command for module at {}, error: {:?}",
                    canonical_module_path.display(),
                    e
                );
                continue;
            }
        };
        module_build_threads.push((module_dir.path(), command_thread_handle));
    }

    let mut modules: Vec<(PathBuf, String)> = vec![];

    for (module_path, module_build_thread_handle) in module_build_threads.into_iter() {
        let output = module_build_thread_handle
            .wait_with_output()
            .with_context(|| {
                format!(
                    "Failed waiting for cargo build command at {}",
                    module_path.display()
                )
            })?;

        if !output.status.success() {
            println!(
                "cargo:warning=Module {} build was unsuccessful, output: {}, err: {}",
                module_path.display(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        } else {
            println!(
                "cargo:warning=Module at {} finished build",
                module_path.display(),
            );

            let module_package_name = {
                let module_cargo_toml_file_path = module_path.join(Path::new("Cargo.toml"));
                let manifest = match Manifest::<Value>::from_path(&module_cargo_toml_file_path) {
                    Ok(manifest) => manifest,
                    Err(e) => return Err(anyhow!("Could not parse module manifest for module at path {}. Internal error is: {:?}", module_cargo_toml_file_path.display(), e)),
                };

                manifest
                    .package
                    .ok_or(anyhow!(
                        "Missing package fields in module {} Cargo.toml",
                        module_path.display()
                    ))?
                    .name
            };

            if let Some((conflicting_module_path, _)) = modules
                .iter()
                .find(|(_, name)| *name == module_package_name)
            {
                return Err(anyhow!(
                    "Module name {} is duplicated, found in {} and {}",
                    module_package_name,
                    conflicting_module_path.display(),
                    module_path.display()
                ));
            }

            modules.push((module_path.clone(), module_package_name.clone()));

            let adjusted_module_package_name = module_package_name.replace('-', "_");

            let wasm_module_file_path = module_path
                .join("target/wasm32-unknown-unknown/debug/")
                .join(adjusted_module_package_name.clone())
                .with_extension("wasm");

            let wasm_module_destination_file_path = module_build_dir_path
                .join(adjusted_module_package_name.clone())
                .with_extension("wasm");

            wait_for_path_creation(&wasm_module_file_path)?;

            fs::copy(&wasm_module_file_path, &wasm_module_destination_file_path).with_context(
                || {
                    format!(
                        "Failed to copy module artifact from {} to {}",
                        wasm_module_file_path.display(),
                        wasm_module_destination_file_path.display()
                    )
                },
            )?;

            println!(
                "cargo:warning=Copied module artifact from {} to {}",
                wasm_module_file_path.display(),
                wasm_module_destination_file_path.display(),
            );
        }
    }

    let mut wasm_tools_threads = vec![];

    // Hacky way to get the file system operations to sync correctly
    std::thread::sleep(std::time::Duration::from_secs(2));

    let module_build_path = Path::new(MODULE_BUILD_DIR)
        .canonicalize()
        .context("Failed to canonicalize module directory path")?;

    for module_file in fs::read_dir(&module_build_path)? {
        let module_file = match module_file {
            Ok(module_file) => module_file.path(),
            Err(e) => {
                println!("cargo:warning=Error traversing module-build directory: {e:?}");
                continue;
            }
        };

        let module_file_name = match module_file.file_name() {
            Some(module_file_name) => module_file_name,

            None => continue,
        };

        let mut wasm_tools_command = {
            let mut c = Command::new("wasm-tools");
            c.arg("component")
                .arg("new")
                .arg(module_file_name)
                .arg("-o")
                .arg(module_file_name)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .current_dir(&module_build_path);
            c
        };

        let command_thread_handle = match wasm_tools_command.spawn() {
            Ok(command_thread_handle) => command_thread_handle,
            Err(e) => {
                println!(
                    "cargo:warning=Error starting wasm-tools command for module at {}, error: {e:?}",
                    module_file.display()
                );
                continue;
            }
        };

        wasm_tools_threads.push((module_file, command_thread_handle));
    }

    for (module_file, wasm_tools_thread_handle) in wasm_tools_threads.into_iter() {
        let output = wasm_tools_thread_handle
            .wait_with_output()
            .with_context(|| {
                format!(
                    "Failed waiting for wasm-tools command at {}",
                    module_file.display()
                )
            })?;

        if !output.status.success() {
            println!(
                "cargo:warning=Module {} component build was unsuccessful, output: {}, err: {}",
                module_file.display(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        } else {
            println!(
                "cargo:warning=Module at {} finished component-ification",
                module_file.display(),
            );
        }
    }

    Ok(())
}

fn main() {
    match build_modules() {
        Ok(_) => {
            println!("cargo:warning=********** Finished Module Build **********",);
        }
        Err(e) => {
            println!("cargo:warning=>>>>> Module build aborted after encountering error: {e}");
        }
    }
}
