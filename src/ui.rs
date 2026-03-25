
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use std::fs::{self, create_dir, create_dir_all};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use crate::config::{ReconConfig, TomlConfig, UserInput, UserProfile, RECON_SETTINGS_FILENAME};
use crate::env::{prompt_yes_no, Environment};
use crate::error::{ReconError};
use headfile::common::ArchiveParams;

#[derive(Serialize, Deserialize)]
/// this struct holds every configuration needed to run a recon
pub struct Reconstruction {
    pub working_directory: PathBuf,
    pub raw_data_base_dir: PathBuf,
    pub run_number: String,
    pub specimen_id: String,
    pub project_code: String,
    pub recon_config: ReconConfig,
    pub user_profile: UserProfile,
    pub archive_params: ArchiveParams,
}

impl TomlConfig for Reconstruction {}

impl Reconstruction {

    pub fn from_user_input(user_input: UserInput) -> Result<Self, ReconError> {
        let (vars, mut recon_config, user_profile, archive_info) = load_settings(&user_input)?;

        let working_directory = vars
            .biggus
            .join(format!("{}.work", user_input.project_code))
            .join(user_input.specimen_id.to_string().replace(':', "-"))
            .join(&user_input.run_number);

        if working_directory.exists() {
            match Self::from_file(working_directory.join(RECON_SETTINGS_FILENAME)) {
                Ok(s) => {
                    println!(
                        "{} already exists. Do you wish to continue the recon?",
                        working_directory.to_string_lossy()
                    );
                    if prompt_yes_no() {
                        return Ok(s);
                    } else {
                        return Err(ReconError::UserCanceled);
                    }
                }
                Err(e) => {
                    println!(
                        "{} already exists but we could not find the settings file!",
                        working_directory.to_string_lossy()
                    );
                    return Err(io::Error::from(ErrorKind::NotFound))?
                }
            }
        }

        let raw_data_base_dir = if let Some(base) = recon_config.object_config.data_host.raw_base_directory.as_ref() {
            if !user_input.raw_data_directory.is_absolute() {
                base.join(user_input.raw_data_directory)
            } else {
                user_input.raw_data_directory.to_path_buf()
            }
        } else {
            user_input.raw_data_directory.to_path_buf()
        };

        if !recon_config.object_config.data_host.scanner().host().dir_exists(&raw_data_base_dir) && recon_config.require_complete_metadata.unwrap_or(true) {
            Err(ReconError::InvalidRawDataSource(raw_data_base_dir.clone()))?
        }

        if !recon_config.object_config.data_host.scanner().host().test_connection() {
            Err(ReconError::SSHConnectionFailed(
                recon_config.object_config.data_host.scanner().host().to_string(),
            ))?
        }

        if let Some(settings) = &user_profile.archive_engine_settings {
            if !settings.computer.test_connection() {
                Err(ReconError::SSHConnectionFailed(
                    settings.computer.to_string(),
                ))?
            }
            if !settings.computer.dir_exists(&settings.base_dir) {
                Err(ReconError::InvalidImageDestination(
                    settings.base_dir.clone(),
                ))?
            }
        }

        let r = Reconstruction {
            working_directory,
            raw_data_base_dir,
            run_number: user_input.run_number,
            specimen_id: user_input.specimen_id,
            project_code: user_input.project_code,
            recon_config,
            user_profile,
            archive_params: archive_info,
        };

        Ok(r)
    }
}

pub fn load_settings(
    user_input: &UserInput,
) -> Result<(Environment, ReconConfig, UserProfile, ArchiveParams), ReconError> {
    let vars = Environment::get().map_err(ReconError::Environment)?;

    let proj_conf = vars.recon_settings.join(&user_input.project_code);
    if !proj_conf.exists() {
        println!(
            "project setting not found at {}",
            proj_conf.to_string_lossy()
        );
        Err(io::Error::from(io::ErrorKind::NotFound))?;
    }

    let recon_profile = proj_conf
        .join(&user_input.config_name)
        .with_extension("toml");
    if !recon_profile.exists() {
        Err(io::Error::from(io::ErrorKind::NotFound))?;
    }
    let recon_config = ReconConfig::from_file(recon_profile)?;
    let user_file = proj_conf.join(&vars.current_user).with_extension("toml");

    let user_profile = if !user_file.exists() {
        let prof = UserProfile::default_from_username(&vars.current_user);
        prof.write_to_file(&user_file)?;
        println!(
            "created a new default user profile : {}",
            user_file.to_string_lossy()
        );
        prof
    } else {
        UserProfile::from_file(user_file)?
    };

    let archive_info = proj_conf.join("archive-info.toml");
    if ArchiveParams::from_file(&archive_info).is_err() {
        ArchiveParams::default()
            .to_file(&archive_info)?;
        println!(
            "WARNING: default meta data was generated at {}",
            archive_info.to_string_lossy()
        );
        println!("you may want to edit this before proceeding")
    }
    let archive_info = ArchiveParams::from_file(archive_info)?;
    Ok((vars, recon_config, user_profile, archive_info))
}

pub fn new_settings_from_template<S: AsRef<str>>(
    new_project_code: S,
    template_project_code: S,
) -> Result<(), ReconError> {
    let vars = Environment::get().map_err(ReconError::Environment)?;

    let template_proj_conf = vars.recon_settings.join(template_project_code.as_ref());
    let new_proj_conf = vars.recon_settings.join(new_project_code.as_ref());
    if template_proj_conf.exists() && template_proj_conf.is_dir() {
        copy_dir_all(template_proj_conf, &new_proj_conf)
            .map_err(|e| ReconError::Generic(e.to_string()))?;
    } else {
        println!(
            "template project {} not found",
            template_proj_conf.to_string_lossy()
        );
        Err(io::Error::from(ErrorKind::NotFound))?;
    }

    println!(
        "project settings copied to {}",
        new_proj_conf.to_string_lossy()
    );

    Ok(())
}

pub fn new_default_settings<S: AsRef<str>>(
    project_code: S,
    recon_config_name: S,
) -> Result<(Environment, ReconConfig, UserProfile, ArchiveParams), ReconError> {
    let vars = Environment::get().map_err(ReconError::Environment)?;

    let proj_conf = vars.recon_settings.join(project_code.as_ref());
    if proj_conf.exists() {
        println!("{} already exists", proj_conf.to_string_lossy());
        Err(ReconError::AlreadyExists(proj_conf.clone()))?;
    }

    create_dir(&proj_conf).map_err(|e| ReconError::Generic(e.to_string()))?;

    let recon_config = ReconConfig::default();
    let user_profile = UserProfile::default_from_username(&vars.current_user);

    let recon_config_filename = proj_conf
        .join(recon_config_name.as_ref())
        .with_extension("toml");
    recon_config
        .write_to_file(&recon_config_filename)?;

    let user_profile_filename = proj_conf.join(&vars.current_user).with_extension("toml");
    user_profile
        .write_to_file(&user_profile_filename)?;
    let archive_info = ArchiveParams::default();

    let archive_info_filename = proj_conf.join("archive-info.toml");
    archive_info
        .to_file(&archive_info_filename)?;

    println!(
        "new project config written to {}",
        proj_conf.to_string_lossy()
    );
    println!(
        "new default archive info: {}",
        archive_info_filename.to_string_lossy()
    );
    println!(
        "new user user profile: {}",
        user_profile_filename.to_string_lossy()
    );
    println!(
        "new recon settings: {}",
        recon_config_filename.to_string_lossy()
    );

    let setting = load_settings(&UserInput {
        project_code: project_code.as_ref().to_string(),
        config_name: recon_config_name.as_ref().to_string(),
        run_number: "dummy".to_string(),
        raw_data_directory: PathBuf::from("dummy"),
        specimen_id: "dummy".to_string(),
        full_cmd: "dummy".to_string(),
        subdirs: None,
    })?;

    Ok(setting)
}

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}