use crate::environment::EnvError;
use crate::recon_pipeline::TaskReturnCode;
use crate::resource_manager::request::RequestError;
use std::error::Error;
use std::fmt::{write, Display};
use std::path::PathBuf;

#[derive(Debug)]
pub enum ReconError {
    ImageReconstruction(ImageReconstructionError),
    ImageWriter(ImageWriterError),
    Resource(ResourceError),
    Preprocessor(Box<dyn Error>),
    Config(ConfigError),
    Generic(Box<dyn Error>),
    SSHConnectionFailed(String),
    InvalidImageDestination(PathBuf),
    InvalidRawDataSource(PathBuf),
    Environment(EnvError),
    AlreadyExists(PathBuf),
    MaxRetriesReached(Box<Self>),
    UserCanceled,
}

pub trait IsRecoverable {
    fn is_recoverable(&self) -> bool;
}

impl ReconError {
    pub fn return_code(&self) -> TaskReturnCode {
        if self.is_recoverable() {
            TaskReturnCode::WillRetry
        } else {
            println!("terminal error encountered: {:?}", self);
            TaskReturnCode::TerminalError
        }
    }
}

impl IsRecoverable for ReconError {
    fn is_recoverable(&self) -> bool {
        match self {
            ReconError::Resource(e) => e.is_recoverable(),
            ReconError::ImageWriter(e) => e.is_recoverable(),
            _ => false,
        }
    }
}

impl IsRecoverable for ResourceError {
    fn is_recoverable(&self) -> bool {
        match self {
            ResourceError::DataRequest(e) => match e {
                RequestError::DataNotReady => true,
                RequestError::FailedToFindMrdFile(_) => true,
                RequestError::FailedToExtractMrdData(_) => true,
                RequestError::FailedToExtractBrukerData(_) => true,
                RequestError::FailedToExtractAgilentData(_) => true,
                _ => false,
            },
            _ => false,
        }
    }
}

impl IsRecoverable for ImageWriterError {
    fn is_recoverable(&self) -> bool {
        match &self {
            ImageWriterError::FailedToSendImages => true,
            ImageWriterError::ScaleFileNotFound(_) => true,
            ImageWriterError::HeadfileIncomplete => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub enum ImageReconstructionError {
    FISTA(Box<dyn Error>),
}

#[derive(Debug)]
pub enum ImageWriterError {
    TooManyDimensions(usize),
    FailedToWriteScaleFile(PathBuf),
    FailedToMakeCivmRaw,
    FailedToSendImages,
    FailedToSendArchiveTag,
    ArchiveEngineNotSpecified,
    ScaleFileNotFound(PathBuf),
    HeadfileIncomplete,
}

#[derive(Debug)]
pub enum ResourceError {
    FailedToFind(PathBuf),
    FailedToLoad(PathBuf),
    FailedToWrite(PathBuf),
    DataRequest(RequestError),
}

#[derive(Debug)]
pub enum ConfigError {
    Read(PathBuf),
    Write(PathBuf),
    Parse(String),
    Serialize,
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            ConfigError::Read(path) => write!(f, "failed to read {}", path.to_string_lossy()),
            ConfigError::Write(path) => write!(f, "failed to write {}", path.to_string_lossy()),
            ConfigError::Serialize => write!(f, "serde serialization failed"),
            ConfigError::Parse(s) => write!(f, "failed to parse: \n {}", s),
        }
    }
}

impl From<EnvError> for ReconError {
    fn from(value: EnvError) -> Self {
        Self::Environment(value)
    }
}

impl From<ResourceError> for ReconError {
    fn from(value: ResourceError) -> Self {
        Self::Resource(value)
    }
}

impl From<ImageReconstructionError> for ReconError {
    fn from(value: ImageReconstructionError) -> Self {
        ReconError::ImageReconstruction(value)
    }
}

impl From<ImageWriterError> for ReconError {
    fn from(value: ImageWriterError) -> Self {
        ReconError::ImageWriter(value)
    }
}

impl From<ConfigError> for ReconError {
    fn from(value: ConfigError) -> Self {
        ReconError::Config(value)
    }
}

impl From<Box<dyn Error>> for ReconError {
    fn from(value: Box<dyn Error>) -> Self {
        Self::Generic(value)
    }
}