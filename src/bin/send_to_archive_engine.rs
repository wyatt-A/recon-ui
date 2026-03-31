use std::path::{Path, PathBuf};
use clap::Parser;
use recon_ui::config::{TomlConfig, UserProfile};
use recon_ui::send_to_archive_engine;

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
    let user_profile = UserProfile::from_file(args.user_profile).unwrap();
    send_to_archive_engine(args.image_dir,&user_profile);
}
