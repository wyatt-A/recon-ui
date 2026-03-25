use std::fmt::Display;
use std::path::PathBuf;
use slurm_interface::JobState;
use recon_ui::config::UserInput;
use recon_ui::env::{prompt_yes_no, Environment, ReconHistoryEntry};
use recon_ui::error::ReconError;

#[derive(clap::Parser, Debug)]
pub struct ReconArgs {
    #[command(subcommand)]
    pub action: ReconAction,
}

#[derive(clap::Subcommand, Debug)]
pub enum ReconAction {
    /// reconstruct a new set of images from scanner data
    New(NewReconArgs),
    /// return a list of previous reconstructions by project code
    Recent(RecentArgs),
    /// check the status of a reconstruction by project code and run number
    Status(StatusArgs),
    /// cancel cluster jobs associated with a run number
    Cancel(CancelArgs),
    /// restart a recon by run number
    Restart(RestartArgs),
    /// return the slurm-*.out file logging standard out
    Watch(WatchArgs),
}

#[derive(clap::Args, Debug)]
pub struct NewReconArgs {
    /// recon configuration name associated with the project code
    config_name: String,
    /// project code associated with images
    project_code: String,
    /// unique run number identifying image set
    run_number: String,
    /// base directory on scanner that hosts raw data
    raw_data_directory: PathBuf,
    /// unique code identifying scanned subject
    specimen_id: String,
    /// disable concurrent work managed by SLURM cluster
    #[clap(long)]
    disable_slurm: bool,
    /// run in serial mode to prevent data fetch from running concurrently.
    /// Only valid if slurm is also disabled
    #[clap(short = 's',long)]
    run_serial: bool,
    /// list of sub-directories to visit to collect raw data. These entries will
    /// overwrite those existing in the config file and is only valid when the file
    /// layout is specified
    #[clap(short = 'd',long,value_delimiter = ',')]
    sub_directories: Option<Vec<String>>
}

#[derive(clap::Args, Debug)]
pub struct CancelArgs {
    /// run number associated with recon task
    run_number: String,
    /// project code associated with recon task
    #[clap(short, long)]
    project_code: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct RestartArgs {
    /// run number associated with recon task
    run_number: String,
    /// project code associated with recon task
    #[clap(short, long)]
    project_code: Option<String>,
    /// disable concurrent work managed by SLURM cluster
    #[clap(long)]
    disable_slurm: bool,
    /// run in serial mode to prevent data fetch from running concurrently.
    /// Only valid if slurm is also disabled
    #[clap(short = 's',long)]
    run_serial: bool,
}

#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// run number associated with recon task
    run_number: String,
    /// project code associated with recon task
    #[clap(short, long)]
    project_code: Option<String>,
    #[clap(short = 's', long)]
    show_all: bool,
}

#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    /// run number associated with recon task
    run_number: String,
    /// index of the pipeline to watch. This can be determined by the status command
    pipeline_index: usize,
    /// project code associated with recon task
    #[clap(short, long)]
    project_code: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct RecentArgs {
    /// project code associated with recon task
    project_code: String,
    /// recon user
    #[clap(short, long)]
    user: Option<String>,
    /// unique code identifying scanned subject
    #[clap(short, long)]
    specimen_id: Option<String>,
    /// max number of run number entries to return
    #[clap(short = 'n', long)]
    max_entries: Option<usize>,
    /// show time stamp
    #[clap(short = 't', long)]
    show_time: bool,
    /// do not filter out duplicate run numbers
    #[clap(short = 'a', long)]
    show_all: bool,
    /// show recon command previously entered
    #[clap(short = 'c', long)]
    show_command: bool
}

fn main() {

}

fn new(args: NewReconArgs) -> Result<(), ReconError> {
    let slurm_disabled = args.disable_slurm;
    let run_serial = args.run_serial;

    let user_input: UserInput = args.into();

    let env = Environment::get().expect("failed to resolve environment");

    // check if the run number has been taken
    if let Some(old_recon) = env.previous_recon(&user_input.run_number) {
        println!("this run number of found previously:");
        println!("{}", old_recon);
        println!("you wish to continue anyway?");
        if !prompt_yes_no() {
            return Err(ReconError::UserCanceled);
        }
    }

    // add new entry to the history log
    let new_entry = ReconHistoryEntry::try_from(user_input.clone())?;
    env.append_recon_history_entry(new_entry);

    if !env.active_projects().is_active(&user_input.project_code) {
        println!(
            "{} not marked as an active project code. Would you like to add it?",
            &user_input.project_code
        );
        if prompt_yes_no() {
            env.add_active_project(&user_input.project_code)
        }
    }
    // launch the recon
    Ok(())
}

struct StatusReportEntry {
    index: usize,
    task_label: String,
    stage_label: String,
    is_complete: bool,
    slurm_status: Option<JobState>,
}

impl Display for StatusReportEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = if self.is_complete {
            "complete"
        } else {
            "incomplete"
        };

        if let Some(slurm_stat) = &self.slurm_status {
            write!(
                f,
                "{}:\t{}\tstage: {}\t{}\tslurm: {}",
                self.index, self.task_label, self.stage_label, s, slurm_stat
            )
        } else {
            write!(
                f,
                "{}:\t{}\tstage: {}\t{}",
                self.index, self.task_label, self.stage_label, s
            )
        }
    }
}

impl From<NewReconArgs> for UserInput {
    fn from(value: NewReconArgs) -> Self {
        let input_string = std::env::args()
            .into_iter()
            .collect::<Vec<String>>()
            .join(" ");
        Self {
            project_code: value.project_code,
            config_name: value.config_name,
            run_number: value.run_number,
            raw_data_directory: value.raw_data_directory,
            specimen_id: value.specimen_id,
            full_cmd: input_string,
            subdirs: value.sub_directories,
        }
    }
}