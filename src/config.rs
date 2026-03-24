use std::path::PathBuf;

use crate::{
    object_manager::kspace_formatting::FormattingMethod,
    preprocessor::preprocessor::{PhaseCorrection, SampleResolver, SignalNormalization},
    recon_manager::recon_manager::ReconAlgorithm,
    resource_manager::{
        resource_manager::{BaseDirExt, FileLayout},
        scanner::Scanner,
    },
};
use cs_lib::FistaReconOptions;
use mr_data::{dim_order::DimOrder, kq_model::KQModel};
use serde::{Deserialize, Serialize};
use super::config::TomlConfig;

use crate::image_writer::image_writer::ArchiveEngineSettings;

use serde::{Deserialize, Serialize};

use super::config::TomlConfig;

use crate::recon_error::ConfigError;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
    time::Duration,
};

pub trait TomlConfig: Serialize + DeserializeOwned {
    fn write_to_file<P: AsRef<Path>>(&self, filename: P) -> Result<(), ConfigError> {
        let filename = filename.as_ref().with_extension("toml");
        let mut f = File::create(&filename).map_err(|_| ConfigError::Write(filename.clone()))?;
        let s = toml::to_string_pretty(self).map_err(|_| ConfigError::Serialize)?;
        f.write_all(s.as_bytes())
            .map_err(|_| ConfigError::Write(filename))?;
        Ok(())
    }

    fn from_file<P: AsRef<Path>>(filename: P) -> Result<Self, ConfigError> {
        let filename = filename.as_ref().with_extension("toml");
        let mut f = File::open(&filename).map_err(|_| ConfigError::Read(filename.clone()))?;
        let mut s = String::new();
        f.read_to_string(&mut s)
            .map_err(|_| ConfigError::Read(filename))?;
        match toml::from_str(&s) {
            Err(e) => {
                println!("{:?}",e);
                return Err(ConfigError::Parse(s))
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

    fn write_to_file<P: AsRef<Path>>(&self, filename: P) -> Result<(), ConfigError> {
        let filename = filename.as_ref().with_extension("state");
        let mut f = File::create(&filename).map_err(|_| ConfigError::Write(filename.clone()))?;
        let s = serde_json::to_string_pretty(self).map_err(|_| ConfigError::Serialize)?;
        f.write_all(s.as_bytes())
            .map_err(|_| ConfigError::Write(filename))?;
        Ok(())
    }

    fn from_file_persistent<P: AsRef<Path>>(
        filename: P,
        total_wait_time_ms: u64,
    ) -> Result<Self, ConfigError> {
        let filename = filename.as_ref().with_extension("state");
        if !filename.exists() {
            return Err(ConfigError::Read(filename.clone()));
        }
        // exponential backoff for reading the file. This is resilient to another process writing
        // to the file, potentially corrupting it
        let n_loads: u32 = 10;
        let b: u64 = (0..n_loads).map(|n| 2u64.pow(n)).sum();
        let wait_const = total_wait_time_ms / b;

        let mut state: Result<Self, ConfigError> = Err(ConfigError::Read(filename.clone()));
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

    fn from_file<P: AsRef<Path>>(filename: P) -> Result<Self, ConfigError> {
        let filename = filename.as_ref().with_extension("state");
        let mut f = File::open(&filename).map_err(|_| ConfigError::Read(filename.clone()))?;
        let mut s = String::new();
        f.read_to_string(&mut s)
            .map_err(|_| ConfigError::Read(filename))?;
        match serde_json::from_str(&s) {
            Ok(t) => return Ok(t),
            Err(e) => {
                println!("err: {:?}",e);
                return Err(ConfigError::Parse(s))
            }
        }
    }
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
    pub scanner: Scanner,
    pub raw_dimension_oder: DimOrder,
    pub raw_file_layout: Option<FileLayout>,
    pub base_dir_ext: Option<BaseDirExt>,
    pub kspace_formatting: FormattingMethod,
    pub phase_correction: Option<PhaseCorrection>,
    pub signal_normalization: Option<SignalNormalization>,
    pub sample_resolver: Option<SampleResolver>,
    pub recon_matrix_size: [usize; 3],
    pub recon_algorithm: ReconAlgorithm,
    pub image_filter_coefficients: Option<[f32; 2]>,
    pub scale_reference_image_index: Option<usize>,
    pub scale_undersaturation_fraction: Option<f32>,
}

impl Default for ReconConfig {
    fn default() -> Self {
        ReconConfig {
            retry_delay_seconds: Some(60*10),
            remote_view_table: None,
            remote_meta_data: None,
            scanner: Scanner::default_mrsolutions(),
            raw_dimension_oder: DimOrder::new(
                &[788, 14420, 3, 67],
                &["samples", "views", "echoes", "experiments"],
            )
                .expect("failed to build dim order"),
            raw_file_layout: Some(FileLayout::OneToOne { n: 67 }),
            base_dir_ext: Some(BaseDirExt::MNumbers { n: 67 }),

            kspace_formatting: FormattingMethod::SingleEchoTest { n_dummy_views: 10 },

            phase_correction: None,
            signal_normalization: None,
            sample_resolver: None,

            recon_matrix_size: [788, 480, 480],
            recon_algorithm: ReconAlgorithm::PhaseMapFISTAL1Wavelet {
                opts: FistaReconOptions::default().max_iter(50),
            },

            image_filter_coefficients: Some([0.15, 0.75]),
            scale_reference_image_index: Some(0),
            scale_undersaturation_fraction: Some(0.9995),
            required_memory_mb: 2_000,
            write_complex: None,
            resampling_table: None,
            require_complete_metadata: None,
            smart_scheduling: None,
        }
    }
}

impl ReconConfig {
    pub fn fse_example() -> Self {
        ReconConfig {
            retry_delay_seconds: Some(60*10),
            scanner: Scanner::default_mrsolutions(),
            raw_dimension_oder: DimOrder::new(
                &[788, 14420, 3, 67],
                &["samples", "views", "echoes", "experiments"],
            )
                .expect("failed to build dim order"),
            raw_file_layout: Some(FileLayout::OneToOne { n: 67 }),
            base_dir_ext: Some(BaseDirExt::MNumbers { n: 67 }),
            kspace_formatting: FormattingMethod::FSE {
                echo_encoding: vec![0, 1, 1],
                n_dummy_views: 10,
                strict_view_assignment: false,
            },
            phase_correction: Some(PhaseCorrection::Basic { sample_radius: 8 }),
            signal_normalization: Some(SignalNormalization::MeanSignal {
                sample_radius: 8,
                scale: vec![1., 0.5, 0.5],
            }),
            sample_resolver: Some(SampleResolver::SumWithRadius {
                sample_radius: 16.0,
                reduce_samples: true,
            }),
            recon_matrix_size: [788, 480, 480],
            recon_algorithm: ReconAlgorithm::PhaseMapFISTAL1Wavelet {
                opts: FistaReconOptions::default().max_iter(50),
            },
            image_filter_coefficients: Some([0.15, 0.75]),
            scale_reference_image_index: Some(0),
            scale_undersaturation_fraction: Some(0.9995),
            required_memory_mb: 2_000,
            write_complex: None,
            resampling_table: None,
            remote_view_table: None,
            remote_meta_data: None,
            require_complete_metadata: None,
            smart_scheduling: None,
        }
    }

    pub fn kq_fse_test_example() -> Self {
        let kqm = PathBuf::from("./environment/kq_models/kqm");
        let radius = KQModel::open(&kqm)
            .expect("failed to load kq model")
            .sample_cutoff;

        ReconConfig {
            retry_delay_seconds: Some(60*10),
            scanner: Scanner::default_mrsolutions(),
            raw_dimension_oder: DimOrder::new(
                &[788, 28800, 3, 67],
                &["samples", "views", "echoes", "experiments"],
            )
                .expect("failed to build dim order"),
            raw_file_layout: Some(FileLayout::OneToOne { n: 67 }),
            base_dir_ext: Some(BaseDirExt::MNumbers { n: 67 }),
            kspace_formatting: FormattingMethod::KQFSETest {
                echo_encoding: vec![0, 0, 0],
                n_dummy_views: 0,
                kq_model: kqm,
            },
            phase_correction: Some(PhaseCorrection::Basic { sample_radius: 8 }),
            signal_normalization: Some(SignalNormalization::MeanSignal {
                sample_radius: 8,
                scale: vec![1., 0.5, 0.5, 1., 0.5, 0.5, 1., 0.5, 0.5],
            }),
            sample_resolver: Some(SampleResolver::SumWithRadius {
                sample_radius: radius,
                reduce_samples: true,
            }),
            recon_matrix_size: [788, 480, 480],
            recon_algorithm: ReconAlgorithm::PhaseMapFISTAL1Wavelet {
                opts: FistaReconOptions::default().max_iter(50),
            },
            image_filter_coefficients: Some([0.15, 0.75]),
            scale_reference_image_index: Some(0),
            scale_undersaturation_fraction: Some(0.9995),
            required_memory_mb: 2_000,
            write_complex: None,
            resampling_table: None,
            remote_view_table: None,
            remote_meta_data: None,
            require_complete_metadata: None,
            smart_scheduling: None,
        }
    }
}

impl TomlConfig for ReconConfig {}


#[cfg(test)]
mod tests {
    use mr_data::{dim_order::DimOrder};
    use crate::{config::config::TomlConfig, object_manager::kspace_formatting::FormattingMethod, recon_manager::{pics_wrapper::BartPicsOptions, recon_manager::ReconAlgorithm}, resource_manager::scanner::Scanner};
    use super::ReconConfig;

    #[test]
    fn agilent_mgre_24mst01() {

        let conf_file = "/Users/Wyatt/workstation/settings/recon/profiles/24.mst.01/mgre.toml";
        let dim_ord = DimOrder::new(&[560,4,12600], &["samples","echoes","views"])
            .unwrap();
        let formatting = FormattingMethod::MultiEcho { n_dummy_views: 0, strict_view_assignment: true };
        let algo = ReconAlgorithm::BartPicsL1Wavelet { opts: BartPicsOptions::default() };
        let conf = ReconConfig {
            retry_delay_seconds: Some(60 * 10),
            resampling_table: None,
            required_memory_mb: 30000,
            write_complex: Some(true),
            scanner: Scanner::default_agilent(),
            raw_dimension_oder: dim_ord,
            raw_file_layout: None,
            base_dir_ext: None,
            kspace_formatting: formatting,
            phase_correction: None,
            signal_normalization: None,
            sample_resolver: None,
            recon_matrix_size: [590,360,360],
            recon_algorithm: algo,
            image_filter_coefficients: Some([0.15,0.75]),
            scale_reference_image_index: Some(0),
            scale_undersaturation_fraction: None,
            remote_view_table: None,
            remote_meta_data: None,
            require_complete_metadata: None,
            smart_scheduling: None,
        };
        conf.write_to_file(conf_file).unwrap();

        ReconConfig::from_file(conf_file).unwrap();

    }

}