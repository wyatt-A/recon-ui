use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use toml;
use object_manager::computer::{Computer};
use object_manager::scanner::Scanner;


use std::{fs::File, io, io::{Read, Write}, path::Path, time::Duration};
use std::io::ErrorKind;
use object_manager::object::ObjectManagerConf;
use recon_lib::ReconMethod;

pub const RECON_SETTINGS_FILENAME: &str = "recon-settings.toml";
pub const SLURM_OUT_DIRNAME: &str = "slurm_out";

pub trait TomlConfig: Serialize + DeserializeOwned {
    fn write_to_file<P: AsRef<Path>>(&self, filename: P) -> Result<(), io::Error> {
        let filename = filename.as_ref().with_extension("toml");
        let mut f = File::create(&filename)?;
        let s = toml::to_string_pretty(self).map_err(|_| io::Error::from(io::ErrorKind::InvalidData) )?;
        f.write_all(s.as_bytes())?;
        Ok(())
    }

    fn from_file<P: AsRef<Path>>(filename: P) -> Result<Self, io::Error> {
        let filename = filename.as_ref().with_extension("toml");
        let mut f = File::open(&filename).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        match toml::from_str(&s) {
            Err(e) => {
                println!("{:?}",e);
                return Err(io::Error::from(io::ErrorKind::InvalidData))
            }
            Ok(t) => return Ok(t)
        }
    }
}

pub trait JsonState: Serialize + DeserializeOwned {
    fn exists(filename: impl AsRef<Path>) -> bool {
        let filename = filename.as_ref().with_extension("state");
        filename.exists()
    }

    fn write_to_file<P: AsRef<Path>>(&self, filename: P) -> Result<(), io::Error> {
        let filename = filename.as_ref().with_extension("state");
        let mut f = File::create(&filename)?;
        let s = serde_json::to_string_pretty(self).map_err(|_| io::Error::from(io::ErrorKind::InvalidData) )?;
        f.write_all(s.as_bytes())?;
        Ok(())
    }

    fn from_file_persistent<P: AsRef<Path>>(
        filename: P,
        total_wait_time_ms: u64,
    ) -> Result<Self, io::Error> {
        let filename = filename.as_ref().with_extension("state");
        if !filename.exists() {
            return Err(io::Error::from(ErrorKind::NotFound));
        }
        // exponential backoff for reading the file. This is resilient to another process writing
        // to the file, potentially corrupting it
        let n_loads: u32 = 10;
        let b: u64 = (0..n_loads).map(|n| 2u64.pow(n)).sum();
        let wait_const = total_wait_time_ms / b;

        let mut state: Result<Self, io::Error> = Err(io::Error::from(io::ErrorKind::InvalidData));
        for i in 0..n_loads {
            match Self::from_file(&filename) {
                Err(err) => {
                    state = Err(err);
                    std::thread::sleep(Duration::from_millis(wait_const * 2u64.pow(i)));
                }
                Ok(s) => return Ok(s),
            }
        }
        state
    }

    fn from_file<P: AsRef<Path>>(filename: P) -> Result<Self, io::Error> {
        let filename = filename.as_ref().with_extension("state");
        let mut f = File::open(&filename)?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        match serde_json::from_str(&s) {
            Ok(t) => return Ok(t),
            Err(e) => {
                println!("err: {:?}",e);
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
        }
    }
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
pub struct UserProfile {
    /// user id commonly returned by whoami or your unique username at your institution
    pub username: String,
    /// settings describing archive computer
    pub archive_engine_settings: Option<ArchiveEngineSettings>,
}

impl UserProfile {
    pub fn default_from_username<S: AsRef<str>>(username: S) -> Self {
        Self {
            username: username.as_ref().to_string(),
            archive_engine_settings: Some(ArchiveEngineSettings::new(
                "delos",
                username.as_ref(),
                "/Volumes/delosspace",
            )),
        }
    }

    pub fn example() -> Self {
        Self {
            username: String::from("wa41"),
            archive_engine_settings: Some(ArchiveEngineSettings::new(
                "delos",
                "wa41",
                "/Volumes/delosspace",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchiveEngineSettings {
    pub base_dir: PathBuf,
    pub computer: Computer,
    pub archive_user: String,
}

impl ArchiveEngineSettings {
    pub fn new<P: AsRef<Path>>(engine_name: &str, engine_user: &str, engine_base_dir: P) -> Self {
        let computer = Computer::new_remote(engine_name, Some(engine_user));
        Self {
            base_dir: engine_base_dir.as_ref().to_path_buf(),
            computer,
            archive_user: engine_user.to_string(),
        }
    }

    pub fn with_archive_user(mut self, archive_user: &str) -> Self {
        self.archive_user = archive_user.to_string();
        self
    }
}

impl TomlConfig for UserProfile {}

#[derive(Serialize, Deserialize)]
pub struct ReconConfig {
    pub retry_delay_seconds: Option<usize>,
    pub smart_scheduling: Option<bool>,
    pub resampling_table: Option<PathBuf>,
    pub remote_view_table: Option<PathBuf>,
    pub remote_meta_data: Option<PathBuf>,
    pub require_complete_metadata: Option<bool>,
    pub required_memory_mb: usize,
    pub write_complex: Option<bool>,
    pub object_config: ObjectManagerConf,
    pub method: ReconMethod,
    pub recon_matrix_size: [usize; 3],
    pub scale_reference_image_index: Option<usize>,
    pub scale_undersaturation_fraction: Option<f32>,
}

impl Default for ReconConfig {
    fn default() -> Self {
        Self {
            retry_delay_seconds: None,
            smart_scheduling: None,
            resampling_table: None,
            remote_view_table: None,
            remote_meta_data: None,
            require_complete_metadata: None,
            required_memory_mb: 1000,
            write_complex: None,
            object_config: Default::default(),
            method: Default::default(),
            recon_matrix_size: [512,256,256],
            scale_reference_image_index: None,
            scale_undersaturation_fraction: None,
        }
    }
}

impl TomlConfig for ReconConfig {}
