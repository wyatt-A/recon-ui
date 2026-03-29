use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::Display,
    fs::{create_dir_all, File},
    io::{self, Write},
    path::{Path, PathBuf},
};
use std::time::SystemTime;
use chrono::{DateTime, Local};
use walkdir::WalkDir;
use crate::config::{JsonState, ReconConfig, TomlConfig, UserInput, UserProfile, RECON_SETTINGS_FILENAME, SLURM_OUT_DIRNAME};
use crate::error::{ReconError};

const BIGGUS: &str = "BIGGUS_DISKUS";
const SETTINGS: &str = "WKS_SETTINGS";
const CACHE: &str = "WKS_CACHE";
const ACTIVE_PROJECTS_FILENAME: &str = "active_projects";
const RECON_HISTORY_FILENAME: &str = "recon_history";

const STRICT_ENV: bool = true;

#[derive(Debug)]
pub enum EnvError {
    CannotGet(String),
    DirNotFound(PathBuf),
    Generic(String),
    FailedToFindRunno(String),
    FailedToFindSettingsFile(PathBuf),
}

pub struct Environment {
    pub biggus: PathBuf,
    pub recon_settings: PathBuf,
    pub recon_cache: PathBuf,
    pub current_user: String,
}

impl Environment {
    pub fn get() -> Result<Self, EnvError> {
        let biggus = match std::env::var(BIGGUS) {
            Ok(biggus) => PathBuf::from(biggus),
            Err(_) => {
                if !STRICT_ENV {
                    return Self::get_fallback_env();
                }
                println!("disable strict env for non-opted-in workstations");
                return Err(EnvError::CannotGet(BIGGUS.to_string()));
            }
        };

        if !biggus.exists() {
            Err(EnvError::DirNotFound(biggus.clone()))?
        }

        let recon_settings = PathBuf::from(
            std::env::var(SETTINGS).map_err(|_| EnvError::CannotGet(SETTINGS.to_string()))?,
        )
            .join("recon")
            .join("profiles");

        if !recon_settings.exists() {
            Err(EnvError::DirNotFound(recon_settings.clone()))?
        }

        let recon_cache = PathBuf::from(
            std::env::var(CACHE).map_err(|_| EnvError::CannotGet(CACHE.to_string()))?,
        )
            .join("recon")
            .join("history");

        if !recon_cache.exists() {
            create_dir_all(&recon_cache).map_err(|e| EnvError::Generic(e.to_string()))?;
        }

        Ok(Self {
            biggus,
            recon_settings,
            current_user: whoami::username().unwrap(),
            recon_cache,
        })
    }

    fn get_fallback_env() -> Result<Self, EnvError> {
        println!("WARNING: falling back to home directory for big disk");

        let biggus = dirs::home_dir().expect("cannot get home directory");
        let recon_settings = biggus.join("recon").join("profiles");
        if !recon_settings.exists() {
            create_dir_all(&recon_settings).map_err(|e| EnvError::Generic(e.to_string()))?;
        }

        let recon_cache = biggus.join("recon").join("history");
        if !recon_cache.exists() {
            create_dir_all(&recon_cache).map_err(|e| EnvError::Generic(e.to_string()))?;
        }

        Ok(Self {
            biggus,
            recon_settings,
            current_user: whoami::username().unwrap(),
            recon_cache,
        })
    }

    /// returns the run-specific reconstruction settings file
    pub fn run_settings<S: AsRef<str>>(
        &self,
        run_number: S,
        project_code: Option<S>,
    ) -> Result<PathBuf, EnvError> {
        let settings_file = self
            .recon_work_dir(run_number, project_code)?
            .join(RECON_SETTINGS_FILENAME);
        if !settings_file.exists() {
            return Err(EnvError::FailedToFindSettingsFile(settings_file));
        }
        Ok(settings_file)
    }

    /// loads recon configuration for a project
    pub fn recon_config(
        &self,
        project_code: impl AsRef<str>,
        config_name: impl AsRef<str>,
    ) -> Result<ReconConfig, ReconError> {
        let config_file = self
            .recon_settings
            .join(project_code.as_ref())
            .join(config_name.as_ref());
        let conf = ReconConfig::from_file(config_file)?;
        Ok(conf)
    }

    /// loads user profile data associated with the project code
    pub fn user_profile(&self,project_code: impl AsRef<str>) -> Result<UserProfile,io::Error> {
        let user_file = self.recon_settings.join(project_code.as_ref()).join(&self.current_user);
        UserProfile::from_file(user_file)
    }

    pub fn slurm_out_directories<S: AsRef<str>>(
        &self,
        run_number: S,
        project_code: Option<S>,
    ) -> Result<Vec<Option<PathBuf>>, EnvError> {
        let pipelines = self.pipeline_configs(run_number, project_code)?;
        let slurm_dirs: Vec<_> = pipelines
            .into_iter()
            .map(|pipeline| {
                let slurm_dir = pipeline.with_file_name(SLURM_OUT_DIRNAME);
                if !slurm_dir.exists() {
                    None
                } else {
                    Some(slurm_dir)
                }
            })
            .collect();
        Ok(slurm_dirs)
    }

    pub fn recon_work_dir<S: AsRef<str>>(
        &self,
        run_number: S,
        project_code: Option<S>,
    ) -> Result<PathBuf, EnvError> {
        match project_code {
            Some(proj_code) => {
                let work = self.runno_work_search(run_number.as_ref(), proj_code.as_ref());
                if work.is_ok() {
                    self.add_active_project(proj_code);
                }
                work
            }
            None => self.search_in_active_projects(run_number.as_ref()),
        }
    }

    fn search_in_active_projects(&self, run_number: &str) -> Result<PathBuf, EnvError> {
        let active_projects = self.active_projects();
        for proj_code in &active_projects.project {
            if let Ok(path) = self.runno_work_search(run_number, proj_code) {
                return Ok(path);
            }
        }
        println!("failed to find run number");
        println!("to help narrow the search, specify the project code, or add it to the active projects manifest:");
        println!("{}", self.active_projects_file().to_string_lossy());
        Err(EnvError::FailedToFindRunno(run_number.to_string()))
    }

    fn runno_work_search<S: AsRef<str>>(
        &self,
        run_number: S,
        project_code: S,
    ) -> Result<PathBuf, EnvError> {
        let search_path = self.biggus.join(format!("{}.work", project_code.as_ref()));
        let max_depth = 3;
        for entry in WalkDir::new(search_path)
            .max_depth(max_depth)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() && entry.file_name() == run_number.as_ref() {
                //println!("found {} belonging to {}",run_number.as_ref(),project_code.as_ref());
                return Ok(entry.into_path());
            }
        }
        Err(EnvError::FailedToFindRunno(run_number.as_ref().to_string()))
    }

    pub fn pipeline_configs<S: AsRef<str>>(
        &self,
        run_number: S,
        project_code: Option<S>,
    ) -> Result<Vec<PathBuf>, EnvError> {
        let search_path = self.recon_work_dir(run_number, project_code)?;
        Ok(Self::find_by_filename(search_path, "recon-pipeline.toml"))
    }

    pub fn recon_pipeline_states<S: AsRef<str>>(
        &self,
        run_number: S,
        project_code: Option<S>,
    ) -> Result<Vec<PathBuf>, EnvError> {
        let search_path = self.recon_work_dir(run_number, project_code)?;
        Ok(Self::find_by_filename(search_path, "recon-pipeline.state"))
    }

    fn find_by_filename(start_path: impl AsRef<Path>, filename: impl AsRef<str>) -> Vec<PathBuf> {
        let mut files: Vec<_> = WalkDir::new(start_path.as_ref())
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && e.file_name() == filename.as_ref())
            .map(|e| e.into_path())
            .collect();
        files.sort();
        files
    }

    /// determine if a project is known to exist
    pub fn project_exists(&self, project_code: impl AsRef<str>) -> bool {
        self.active_projects().is_active(project_code)
    }

    /// add an active project to the manifest. Nothing is done if it already exists
    pub fn add_active_project(&self, project_code: impl AsRef<str>) {
        let mut ap = self.active_projects();
        if ap.add_project(project_code) {
            ap.write_to_file(self.active_projects_file())
                .expect("failed to write to active projects file!");
        }
    }

    fn active_projects_file(&self) -> PathBuf {
        self.recon_settings
            .join(ACTIVE_PROJECTS_FILENAME)
            .with_extension("toml")
    }

    /// get currently active projects
    pub fn active_projects(&self) -> ActiveProjects {
        let active_projects_file = self.active_projects_file();
        if !active_projects_file.exists() {
            let active_projects = ActiveProjects::default();
            active_projects
                .write_to_file(&active_projects_file)
                .expect("failed to write to active projects file");
            return active_projects;
        } else {
            match ActiveProjects::from_file(&active_projects_file) {
                Ok(ap) => return ap,
                Err(_) => {
                    // the case where the file is corrupted. We nuke it and start over
                    println!("failed to load active projects... resetting to default");
                    let active_projects = ActiveProjects::default();
                    active_projects
                        .write_to_file(&active_projects_file)
                        .expect("failed to write to active projects file");
                    return active_projects;
                }
            }
        }
    }

    /// get the recon history details associated with a run number if they exist
    pub fn previous_recon(&self, run_number: impl AsRef<str>) -> Option<ReconHistoryEntry> {
        let history = self.recon_history();
        history.entry_lookup(run_number)
    }

    /// get recon history
    pub fn recon_history(&self) -> ReconHistory {
        let recon_history_file = self.recon_history_file();
        if !ReconHistory::exists(&recon_history_file) {
            let recon_history = ReconHistory::default();
            recon_history
                .write_to_file(&recon_history_file)
                .expect("failed to write to recon history file");
            return recon_history;
        } else {
            ReconHistory::from_file(&recon_history_file)
                .expect("failed to parse recon history file")
        }
    }

    fn recon_history_file(&self) -> PathBuf {
        self.recon_cache.join(RECON_HISTORY_FILENAME)
    }

    pub fn append_recon_history_entry(&self, entry: ReconHistoryEntry) {
        let mut history = self.recon_history();
        history.append(entry);
        history
            .write_to_file(self.recon_history_file())
            .expect("failed to write to recon history file");
    }
}

#[derive(Serialize, Deserialize)]
pub struct ActiveProjects {
    pub project: Vec<String>,
}

impl TomlConfig for ActiveProjects {}

impl Default for ActiveProjects {
    fn default() -> Self {
        Self { project: vec![] }
    }
}

impl ActiveProjects {
    pub fn is_active(&self, project_code: impl AsRef<str>) -> bool {
        self.project.contains(&project_code.as_ref().to_string())
    }

    /// add project to the list, returning false if the project already exists
    pub fn add_project(&mut self, project_code: impl AsRef<str>) -> bool {
        if self.is_active(project_code.as_ref()) {
            return false;
        } else {
            self.project.push(project_code.as_ref().to_string());
            return true;
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ReconHistory {
    history: Vec<ReconHistoryEntry>,
}

impl ReconHistory {
    pub fn append(&mut self, entry: ReconHistoryEntry) {
        self.history.push(entry);
    }
    pub fn contains_run_number(&self, run_number: impl AsRef<str>) -> bool {
        let runno_set =
            HashSet::<&str>::from_iter(self.history.iter().map(|h| h.run_number.as_str()));
        runno_set.contains(run_number.as_ref())
    }

    /// return the most recent entry
    pub fn entry_lookup(&self, run_number: impl AsRef<str>) -> Option<ReconHistoryEntry> {
        let set = HashMap::<&str, &ReconHistoryEntry>::from_iter(
            self.history.iter().map(|h| (h.run_number.as_str(), h)),
        );
        set.get(run_number.as_ref())
            .cloned()
            .map(|entry| entry.clone())
    }

    pub fn filter_by_project(mut self, project_code: impl AsRef<str>) -> Self {
        self.filter_by_project_mut(project_code);
        self
    }

    pub fn filter_by_project_mut(&mut self, project_code: impl AsRef<str>) {
        self.history
            .retain(|h| h.project_code.as_str() == project_code.as_ref());
    }

    pub fn filter_by_user_mut(&mut self, user: impl AsRef<str>) {
        self.history.retain(|h| h.user.as_str() == user.as_ref());
    }

    pub fn filter_by_user(mut self, user: impl AsRef<str>) -> Self {
        self.filter_by_user_mut(user);
        self
    }

    pub fn filter_by_specimen(mut self, specimen_id: impl AsRef<str>) -> Self {
        self.filter_by_specimen_mut(specimen_id);
        self
    }

    pub fn filter_by_specimen_mut(&mut self, specimen_id: impl AsRef<str>) {
        self.history
            .retain(|h| h.specimen_id.as_str() == specimen_id.as_ref());
    }

    pub fn sort_by_date(&mut self) {
        self.history.sort_by(|a, b| a.date.cmp(&b.date));
    }

    pub fn remove_duplicate_runnos(&mut self) {
        self.sort_by_date();
        let set = HashMap::<&str, &ReconHistoryEntry>::from_iter(
            self.history.iter().map(|h| (h.run_number.as_str(), h)),
        );
        let mut entries: Vec<_> = set
            .iter()
            .map(|(_, val)| val.to_owned().to_owned())
            .collect();
        entries.sort_by(|a, b| a.date.cmp(&b.date));
        self.history = entries;
    }

    pub fn run_numbers(&self) -> Vec<String> {
        self.history.iter().map(|h| h.run_number.clone()).collect()
    }

    pub fn dates(&self) -> Vec<String> {
        self.history.iter().map(|h| h.date.clone()).collect()
    }

    pub fn commands(&self) -> Vec<String> {
        self.history.iter().map(|h| h.command.clone()).collect()
    }
}

impl JsonState for ReconHistory {}

impl Default for ReconHistory {
    fn default() -> Self {
        Self { history: vec![] }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ReconHistoryEntry {
    run_number: String,
    project_code: String,
    user: String,
    specimen_id: String,
    date: String,
    raw_data: String,
    data_host: String,
    command: String,
    recon_config: String,
}

pub fn time_stamp() -> String {
    let datetime: DateTime<Local> = SystemTime::now().into();
    format!("{}",datetime.format("%Y%m%d:%T"))
}

impl TryFrom<UserInput> for ReconHistoryEntry {
    type Error = ReconError;

    fn try_from(value: UserInput) -> Result<Self, Self::Error> {
        let env = Environment::get()?;
        let date = time_stamp();
        let data_host = env
            .recon_config(&value.project_code, &value.config_name)?
            .object_config.data_host.scanner()
            .host()
            .hostname()
            .unwrap_or("unknown".to_string());

        Ok(Self {
            run_number: value.run_number,
            project_code: value.project_code,
            user: env.current_user,
            specimen_id: value.specimen_id,
            date,
            raw_data: value.raw_data_directory.to_string_lossy().to_string(),
            command: value.full_cmd,
            recon_config: value.config_name,
            data_host,
        })
    }
}

impl Display for ReconHistoryEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "
            run number: {}
            project code: {}
            config: {}
            user: {}
            specimen id: {}
            date: {}
            raw data: {}
            data host: {}
            args: {}
        ",
            self.run_number,
            self.project_code,
            self.recon_config,
            self.user,
            self.specimen_id,
            self.date,
            self.raw_data,
            self.data_host,
            self.command
        )
    }
}

pub fn prompt_yes_no() -> bool {
    loop {
        print!("Please enter yes (Y) or no (n): ");
        io::stdout().flush().expect("Failed to flush stdout");
        let input = get_user_input().to_ascii_lowercase();
        match input.as_str() {
            "yes" | "y" => return true,
            "no" | "n" => return false,
            _ => {
                println!("Invalid input. Please try again.");
            }
        }
    }
}

fn get_user_input() -> String {
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input.trim().to_string()
}