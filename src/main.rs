mod env;

use crate::config::config::{JsonState, TomlConfig};
use crate::config::recon_config::ReconConfig;
use crate::config::user_profile::UserProfile;
use crate::environment::{prompt_yes_no, Environment};
use crate::recon_error::ConfigError;
use crate::recon_pipeline::{ReconPipeline, SLURM_OUT_DIRNAME};
use crate::resource_manager::resource_manager::BaseDirExt;
use crate::slurm::slurm::{self, ClusterTask};
use crate::{
    data_server::pipeline_component::PipelineComponent,
    image_writer::image_writer::ImageWriter,
    object_manager::object_manager::ObjectManager,
    preprocessor::preprocessor::{
        PhaseCorrection, Preprocessor, SampleResolver, SignalNormalization,
    },
    recon_error::ReconError,
    recon_manager::recon_manager::ReconManager,
    resource_manager::resource_manager::ResourceManager,
};
use civm_rust_utils::m_number_formatter;
use headfile::headfile::ArchiveInfo;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use std::fs::{self, create_dir, create_dir_all};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

pub const RECON_SETTINGS_FILENAME: &str = "recon-settings.toml";


fn main() {

}

#[derive(Clone)]
pub struct UserInput {
    pub project_code: String,
    pub config_name: String,
    pub run_number: String,
    pub raw_data_directory: PathBuf,
    pub specimen_id: String,
    pub full_cmd: String,
    pub subdirs: Option<Vec<String>>,
}

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
    pub archive_info: ArchiveInfo,
    pub processes: Vec<ReconPipeline>,
}

impl TomlConfig for Reconstruction {}
impl TomlConfig for ArchiveInfo {}

impl Reconstruction {

    pub fn run(&mut self, slurm_disabled: bool, run_serial:bool) -> Result<(), ReconError> {

        if slurm::is_installed() && !slurm_disabled {
            let mut n_launched_processes = 0;
            self.processes.iter_mut().for_each(|p| {
                let mut state = p.load_state().expect("failed to load state");
                if !state.is_complete() {
                    let jid = p.launch_slurm_now();
                    state.job_id = Some(jid);
                    p.save_state(&state).expect("failed to save state");
                    n_launched_processes += 1;
                }
            });
            if n_launched_processes > 1 {
                println!(
                    "successfully launched {} recon pipelines",
                    n_launched_processes
                );
            }else if n_launched_processes == 1 {
                println!(
                    "successfully launched 1 recon pipeline",
                );
            }
        } else {
            // get the first fetch pipeline to run concurrently
            let mut data_fetch = self.processes.remove(0);
            let data_fetch_handle = if run_serial {
                data_fetch.launch(true).expect("data fetch failed");
                None
            }else {
                let data_fetch_handle = std::thread::spawn(move || {
                    data_fetch.launch(true).expect("data fetch failed");
                });
                Some(data_fetch_handle)
            };
            self.processes.iter_mut().for_each(|p| {
                p.launch(true).expect("proc failed");
            });
            if let Some(data_fetch_handle) = data_fetch_handle {
                data_fetch_handle
                    .join()
                    .expect("failed to recover data-fetch thread. It may have failed");
            }
        }

        Ok(())
    }

    pub fn from_user_input(user_input: UserInput) -> Result<Self, ReconError> {
        let (vars, mut recon_config, user_profile, archive_info) = load_settings(&user_input)?;

        // this handles the case where the user specifies an alternate sub-directory structure for the raw data
        if let Some(subdirs) = user_input.subdirs.as_ref() {
            if recon_config.base_dir_ext.is_some() {
                warn!("overwriting base directory extensions with {:?}",subdirs);
            }
            let (_,&last_size) = recon_config.raw_dimension_oder.last();
            let obj_layout = recon_config.raw_file_layout.clone().map(|x|x.to_vec()).unwrap_or(vec![last_size]);
            if obj_layout.len() != subdirs.len() {
                panic!("the number of sub directories ({}) must equal the number of specified raw files ({})",
                       subdirs.len(),obj_layout.len()
                )
            }
            recon_config.base_dir_ext = Some(BaseDirExt::Labeled { labels: subdirs.to_owned() })
        }

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
                    return Err(ReconError::Config(e));
                }
            }
        }

        let raw_data_base_dir = if let Some(base) = recon_config.scanner.base_dir() {
            if !user_input.raw_data_directory.is_absolute() {
                base.join(user_input.raw_data_directory)
            } else {
                user_input.raw_data_directory.to_path_buf()
            }
        } else {
            user_input.raw_data_directory.to_path_buf()
        };

        if !recon_config.scanner.host().dir_exists(&raw_data_base_dir) && recon_config.require_complete_metadata.unwrap_or(true) {
            Err(ReconError::InvalidRawDataSource(raw_data_base_dir.clone()))?
        }

        if !recon_config.scanner.host().test_connection() {
            Err(ReconError::SSHConnectionFailed(
                recon_config.scanner.host().to_string(),
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

        let mut r = Reconstruction {
            working_directory,
            raw_data_base_dir,
            run_number: user_input.run_number,
            specimen_id: user_input.specimen_id,
            project_code: user_input.project_code,
            recon_config,
            user_profile,
            archive_info,
            processes: vec![],
        };

        r.compile()?;

        Ok(r)
    }

    pub fn compile(&mut self) -> Result<(), ReconError> {
        create_dir_all(&self.working_directory).map_err(|e| ReconError::Generic(e.into()))?;

        let resource_dir = self.working_directory.join("resource");
        create_dir(&resource_dir).map_err(|e| ReconError::Generic(e.into()))?;

        let mut re = ResourceManager::new(
            &resource_dir,
            &self.raw_data_base_dir,
            &self.recon_config.raw_dimension_oder,
            self.recon_config.scanner.clone(),
        );

        if let Some(ext) = &self.recon_config.base_dir_ext {
            re.with_dir_extensions_mut(ext.clone());
        }

        if let Some(layout) = &self.recon_config.raw_file_layout {
            re.with_file_layout_mut(layout.clone());
        }

        if let Some(remote_view_table) = &self.recon_config.remote_view_table {
            re.with_view_table_path_mut(remote_view_table);
        }

        if let Some(remote_meta_data) = &self.recon_config.remote_meta_data {
            re.with_meta_data_path_mut(remote_meta_data);
        }

        PipelineComponent::write_to_file(&re).map_err(ReconError::Config)?;

        let object_dir = self.working_directory.join("object-data");
        create_dir(&object_dir).ok();

        let data_prep_name = format!("{}-data-fetch", self.run_number);

        let mut obj = ObjectManager::new(&object_dir, &re, &self.recon_config.kspace_formatting);
        if let Some(is_required) = self.recon_config.require_complete_metadata {
            obj.require_complete_metadata(is_required);
        }

        let mut concurrent_work = vec![];

        if self.recon_config.smart_scheduling.unwrap_or(false) {
            // give object manager control of spawning recon jobs
            obj.set_child_pipelines(
                &self.compile_volume_jobs(&obj, &re)?
            );
            PipelineComponent::write_to_file(&obj).map_err(ReconError::Config)?;
            let data_prep_pipline =
                ReconPipeline::new(data_prep_name,
                                   0,
                                   self.recon_config.retry_delay_seconds.unwrap_or(10 * 60),
                                   &self.working_directory,
                                   self.recon_config.required_memory_mb,
                )
                    .add_process(&obj);
            PipelineComponent::write_to_file(&data_prep_pipline).map_err(ReconError::Config)?;
            concurrent_work.push(data_prep_pipline)
        }else {
            // create seperate concurrent units of work
            PipelineComponent::write_to_file(&obj).map_err(ReconError::Config)?;
            let data_prep_pipline =
                ReconPipeline::new(data_prep_name,
                                   0,
                                   self.recon_config.retry_delay_seconds.unwrap_or(10 * 60),
                                   &self.working_directory,
                                   self.recon_config.required_memory_mb,
                )
                    .add_process(&obj);
            PipelineComponent::write_to_file(&data_prep_pipline).map_err(ReconError::Config)?;
            concurrent_work.push(data_prep_pipline);
            concurrent_work.extend_from_slice(
                &self.compile_volume_jobs(&obj, &re)?
            );
        }

        self.processes = concurrent_work;

        let settings_file = self.working_directory.join(RECON_SETTINGS_FILENAME);
        self.write_to_file(&settings_file)?;

        Ok(())
    }

    fn compile_volume_jobs(&self,obj:&ObjectManager,re:&ResourceManager) -> Result<Vec<ReconPipeline>,ReconError> {
        let n_objs = obj.total_objects().unwrap();
        let mut concurrent_work = vec![];
        for (i, mnum) in m_number_formatter(n_objs).into_iter().enumerate() {
            let work_dir = self.working_directory.join(&mnum);
            if !work_dir.exists() {
                create_dir(&work_dir).map_err(|e| ReconError::Generic(e.into()))?
            }
            let prep = Preprocessor::new(
                &work_dir,
                i,
                &obj,
                self.recon_config.recon_matrix_size,
                self.recon_config
                    .signal_normalization
                    .as_ref()
                    .unwrap_or(&SignalNormalization::None),
                self.recon_config
                    .phase_correction
                    .as_ref()
                    .unwrap_or(&PhaseCorrection::None),
                self.recon_config
                    .sample_resolver
                    .as_ref()
                    .unwrap_or(&SampleResolver::Strict),
                self.recon_config.resampling_table.clone(),
            );
            PipelineComponent::write_to_file(&prep).unwrap();
            let reco = ReconManager::new(
                &work_dir,
                &prep,
                self.recon_config.recon_matrix_size,
                &self.recon_config.recon_algorithm,
                self.recon_config.image_filter_coefficients,
            );
            PipelineComponent::write_to_file(&reco).unwrap();
            let image_writer = ImageWriter::new(
                &work_dir,
                &re,
                &obj,
                &reco,
                &self.run_number,
                &self.specimen_id,
                &self.user_profile.username,
                &self.project_code,
                &"imx".to_string(),
                &self.recon_config.scanner.image_code(),
                self.archive_info.clone(),
                self.user_profile.archive_engine_settings.clone(),
                self.recon_config.scale_reference_image_index.unwrap_or(0),
                self.recon_config
                    .scale_undersaturation_fraction
                    .unwrap_or(0.9995),
                self.recon_config.write_complex.unwrap_or(false),
            );
            PipelineComponent::write_to_file(&image_writer).unwrap();
            let pipeline_name = format!("{}_{}", self.run_number, mnum);
            let pipeline = ReconPipeline::new(
                pipeline_name,
                i+1,
                self.recon_config.retry_delay_seconds.unwrap_or(10 * 60),
                work_dir,
                self.recon_config.required_memory_mb,
            )
                .add_process(&prep)
                .add_process(&reco)
                .add_process(&image_writer);
            PipelineComponent::write_to_file(&pipeline).map_err(ReconError::Config)?;
            concurrent_work.push(pipeline);
        }
        Ok(concurrent_work)
    }
}

pub fn load_settings(
    user_input: &UserInput,
) -> Result<(Environment, ReconConfig, UserProfile, ArchiveInfo), ReconError> {
    let vars = Environment::get().map_err(ReconError::Environment)?;

    let proj_conf = vars.recon_settings.join(&user_input.project_code);
    if !proj_conf.exists() {
        println!(
            "project setting not found at {}",
            proj_conf.to_string_lossy()
        );
        Err(ReconError::Config(ConfigError::Read(proj_conf.clone())))?;
    }

    let recon_profile = proj_conf
        .join(&user_input.config_name)
        .with_extension("toml");
    if !recon_profile.exists() {
        Err(ReconError::Config(ConfigError::Read(recon_profile.clone())))?;
    }
    let recon_config = ReconConfig::from_file(recon_profile).map_err(ReconError::Config)?;
    let user_file = proj_conf.join(&vars.current_user).with_extension("toml");

    let user_profile = if !user_file.exists() {
        let prof = UserProfile::default_from_username(&vars.current_user);
        prof.write_to_file(&user_file).map_err(ReconError::Config)?;
        println!(
            "created a new default user profile : {}",
            user_file.to_string_lossy()
        );
        prof
    } else {
        UserProfile::from_file(user_file).map_err(ReconError::Config)?
    };

    let archive_info = proj_conf.join("archive-info.toml");
    if ArchiveInfo::from_file(&archive_info).is_err() {
        ArchiveInfo::default()
            .write_to_file(&archive_info)
            .map_err(ReconError::Config)?;
        println!(
            "WARNING: default meta data was generated at {}",
            archive_info.to_string_lossy()
        );
        println!("you may want to edit this before proceeding")
    }
    let archive_info = ArchiveInfo::from_file(archive_info).map_err(ReconError::Config)?;
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
            .map_err(|e| ReconError::Generic(e.into()))?;
    } else {
        println!(
            "template project {} not found",
            template_proj_conf.to_string_lossy()
        );
        Err(ReconError::Config(ConfigError::Read(template_proj_conf)))?;
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
) -> Result<(Environment, ReconConfig, UserProfile, ArchiveInfo), ReconError> {
    let vars = Environment::get().map_err(ReconError::Environment)?;

    let proj_conf = vars.recon_settings.join(project_code.as_ref());
    if proj_conf.exists() {
        println!("{} already exists", proj_conf.to_string_lossy());
        Err(ReconError::Config(ConfigError::Write(proj_conf.clone())))?;
    }

    create_dir(&proj_conf).map_err(|e| ReconError::Generic(e.into()))?;

    let recon_config = ReconConfig::default();
    let user_profile = UserProfile::default_from_username(&vars.current_user);

    let recon_config_filename = proj_conf
        .join(recon_config_name.as_ref())
        .with_extension("toml");
    recon_config
        .write_to_file(&recon_config_filename)
        .map_err(ReconError::Config)?;

    let user_profile_filename = proj_conf.join(&vars.current_user).with_extension("toml");
    user_profile
        .write_to_file(&user_profile_filename)
        .map_err(ReconError::Config)?;
    let archive_info = ArchiveInfo::default();

    let archive_info_filename = proj_conf.join("archive-info.toml");
    archive_info
        .write_to_file(&archive_info_filename)
        .map_err(ReconError::Config)?;

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