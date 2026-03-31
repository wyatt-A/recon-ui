
use std::path::PathBuf;
use clap::Parser;
use serde::Serialize;
use recon_ui::config::{TomlConfig, UserProfile};
use glob;
use headfile::Headfile;

#[derive(Clone, Debug, Parser)]
struct Args {
    image_dir: PathBuf,
    user_profile: PathBuf,
    timeout_sec: Option<u64>,
    debug:bool,
    no_archive:bool,
}

fn main() {

    let args = Args::parse();

    let user_profile = UserProfile::from_file(&args.user_profile).unwrap();


    // get project code and specimen ID from headfile
    let pat = args.image_dir.join("*.headfile");
    let g:Vec<_> = glob::glob(pat.display().to_string().as_str())
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok).collect();

    if g.is_empty() {
        panic!("Failed to find .headfile in {}",args.image_dir.display());
    }

    let hf = &g[0];

    println!("reading {}",hf.display());

    let h = Headfile::from_file(&hf).unwrap();
    let proj_code = h.project_code().expect(&format!("failed to get project code from {}",hf.display()));
    let spec_id = h.specimen_id().expect(&format!("failed to get specimen id from {}",hf.display()))
        .replace(":","-");
    let base_runno = h.base_run_number().expect("failed to extract base run number from headfile");

    if let Some(archive_engine) = user_profile.archive_engine_settings.as_ref() {

        if archive_engine.computer.test_connection() {
            let dst = archive_engine.base_dir.join(&proj_code).join(&spec_id).join(base_runno);
            archive_engine.computer
                .run_command("mkdir", vec!["-p", dst.to_str().unwrap()], false)
                .expect("failed to create directory on remote");
            archive_engine.computer.push_dir(&dst,&args.image_dir);
        }else {
            panic!("failed to connect to archive engine");
        }

    }else {
        panic!("no archive engine defined in user profile");
    }

}