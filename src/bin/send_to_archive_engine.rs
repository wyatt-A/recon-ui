
use std::path::PathBuf;
use clap::Parser;
use serde::Serialize;
use recon_ui::config::{TomlConfig, UserProfile};
use glob;
use headfile::archive_tag::ArchiveTag;
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
    let full_runno = h.run_number().expect("failed to extract run number from headfile");
    let base_runno = h.base_run_number().expect("failed to extract base run number from headfile");
    let fmt = h.raw_fmt().expect("failed to get raw fmt from headfile");
    let dim_z = h.get_numeric_scalar("dim_Z").expect("failed to get dim_Z from headfile") as usize;







    if let Some(archive_engine) = user_profile.archive_engine_settings.as_ref() {

        let img_dst = archive_engine.base_dir.join(&proj_code).join(&spec_id).join(base_runno);

        let tag = ArchiveTag {
            runno: full_runno,
            civm_id: archive_engine.archive_user.clone(),
            archive_engine_base_dir: img_dst.clone(),
            n_raw_files: dim_z,
            project_code: proj_code.clone(),
            raw_file_ext: fmt.clone(),
        };

        if archive_engine.computer.test_connection() {

            archive_engine.computer
                .run_command("mkdir", vec!["-p", img_dst.to_str().unwrap()], false)
                .expect("failed to create directory on remote");
            archive_engine.computer.push_dir(&img_dst, &args.image_dir);

            if args.no_archive {
                return
            }

            let tf = tag.filepath(args.image_dir.parent().unwrap());
            tag.to_file(args.image_dir.parent().unwrap());
            let tag_dir = archive_engine.base_dir.join("Archive_Tags");
            if !archive_engine.computer.push_file(&tag_dir, &tf) {
                panic!("Failed to push archive tag to {}",tag_dir.display());
            }

        }else {
            panic!("failed to connect to archive engine");
        }

    }else {
        panic!("no archive engine defined in user profile");
    }

}

// fn archive_tag(
//     &self,
//     image_index: usize,
//     dim_z: usize,
//     is_complex: bool,
// ) -> Result<ArchiveTag, ReconError> {
//     if let Some(engine) = &self.archive_engine_settings {
//         let file_ext = if is_complex {
//             "cf32"
//         }else {
//             "raw"
//         };
//         let tag = ArchiveTag {
//             runno: self.image_label(image_index, is_complex)?,
//             civm_id: engine.archive_user.clone(),
//             archive_engine_base_dir: self.remote_results_dir().unwrap(),
//             n_raw_files: dim_z,
//             project_code: self.project_code.to_owned(),
//             raw_file_ext: file_ext.to_string(),
//         };
//         Ok(tag)
//     } else {
//         Err(ReconError::ImageWriter(
//             ImageWriterError::ArchiveEngineNotSpecified,
//         ))?
//     }
// }