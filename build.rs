use std::env;
use std::process::Command;

fn main() {
    let root_dir = std::path::PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let mycelite_dir = {
        let mut dir = root_dir.clone();
        dir.push("mycelite");
        dir
    };
    let profile = env::var("PROFILE").unwrap();
    let build_profile = match profile.as_str() {
        "debug" => "dev".into(),
        p => p,
    };
    let output_dir = output_dir(root_dir.as_path(), profile.as_str());
    let mycelite_output_dir = mycelite_output_dir(root_dir.as_path(), profile.as_str());

    println!("root_dir: {root_dir:?}");
    println!("mycelite_dir: {mycelite_dir:?}");
    println!("profile: {profile}");
    println!("build profile: {build_profile}");
    println!("output_dir: {output_dir:?}");
    println!("mycelite_output_dir: {mycelite_output_dir:?}");


    let mut build_cmd = Command::new("cargo");
    build_cmd
        .env_clear()
        .env("PATH", env::var("PATH").unwrap())
        .current_dir(mycelite_dir.as_path())
        .arg("build")
        .arg(format!("--profile={build_profile}"));
    println!("build command: {build_cmd:?}");
    build_cmd
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    let mycelite_so_path = mycelite_so_path(mycelite_output_dir.as_path());
    println!("mycelite_so_path: {mycelite_so_path:?}");

    let mut copy_cmd = Command::new("cp");
    copy_cmd
        .arg(mycelite_so_path.as_path())
        .arg(mycelite_so_output_path(mycelite_so_path.as_path(), output_dir.as_path()));
    println!("copy command: {copy_cmd:?}");
    copy_cmd
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

}


fn output_dir(root_dir: &std::path::Path, profile: &str) -> std::path::PathBuf {
    let mut output_dir: std::path::PathBuf = root_dir.into();
    output_dir.push("target");
    output_dir.push(profile);
    output_dir
}

fn mycelite_output_dir(root_dir: &std::path::Path, profile: &str) -> std::path::PathBuf {
    let mut output_dir: std::path::PathBuf = root_dir.into();
    output_dir.push("mycelite");
    output_dir.push("target");
    output_dir.push(profile);
    output_dir
}

fn mycelite_so_path(output_dir: &std::path::Path) -> std::path::PathBuf {
    let mut path: std::path::PathBuf = output_dir.into();
    let mut found = false;
    println!("path: {:?}", path);
    for entry in std::fs::read_dir(path.as_path()).unwrap().map(|e| e.unwrap()) {
        match entry.file_name().as_os_str().to_str().unwrap() {
            e if e == "libmycelite_dynamic.so" || e == "libmycelite_dynamic.dylib" => {
                path.push(e);
                found = true;
            },
            _ => (),
        }
    }
    if !found {
        panic!("couldn't to find shared object file");
    }
    path
}

fn mycelite_so_output_path(so_path: &std::path::Path, output_dir: &std::path::Path) -> std::path::PathBuf {
    let extension = so_path.extension().unwrap().to_str().unwrap();
    let mut output: std::path::PathBuf = output_dir.into();
    output.push(format!("libmycelite.{extension}"));
    output
}
