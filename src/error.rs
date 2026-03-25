
use std::error::Error;
use std::fmt::{write, Display};
use std::io;
use std::path::PathBuf;
use object_manager::RequestError;
use crate::env::EnvError;

#[derive(Debug)]
pub enum ReconError {
    ImageReconstruction(ImageReconstructionError),
    ImageWriter(ImageWriterError),
    Resource(ResourceError),
    Preprocessor(String),
    Config(String),
    Generic(String),
    SSHConnectionFailed(String),
    InvalidImageDestination(PathBuf),
    InvalidRawDataSource(PathBuf),
    Environment(EnvError),
    AlreadyExists(PathBuf),
    MaxRetriesReached(String),
    UserCanceled,
    IO(String),
}

impl From<io::Error> for ReconError {
    fn from(value: io::Error) -> Self {
        Self::IO(value.to_string())
    }
}

pub trait IsRecoverable {
    fn is_recoverable(&self) -> bool;
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
