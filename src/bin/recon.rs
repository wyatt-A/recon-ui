use std::fmt::Display;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::process::Command;
use array_lib::io_cfl::write_cfl;
use clap::Parser;
use headfile::common::ReconHeadfileParams;
use headfile::Headfile;
use object_manager::object::ObjectManager;
use object_manager::{JsonState, TomlConf};
use object_manager::request::RequestType;
use recon_lib::{run_cs_cartesian, ReconMethod};
use slurm_interface::{JobState, SlurmTask};
use recon_ui::config::{ReconConfig, TomlConfig, UserInput};
use recon_ui::env::{prompt_yes_no, Environment, ReconHistoryEntry};
use recon_ui::error::ReconError;
use recon_ui::ui::load_settings;
use crate::ReconAction::New;

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

    NewConfig(NewConfigArgs),
}

#[derive(clap::Args, Debug)]
pub struct NewConfigArgs {
    project_code: String,
    config_name: String,
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
    /// list of subdirectories to visit to collect raw data. These entries will
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

fn main() -> Result<(), ReconError> {

    let args = ReconArgs::parse();
    match args.action {
        ReconAction::New(args) => new(args)?,
        ReconAction::NewConfig(args) => new_config(args)?,
        _=> {todo!()}
    }

    Ok(())

}


fn new_config(args:NewConfigArgs) -> Result<(), ReconError> {
    let env = Environment::get().expect("failed to resolve environment");
    let proj_dir = env.recon_settings.join(&args.project_code);
    let conf_file = proj_dir.join(args.config_name).with_extension("toml");
    create_dir_all(proj_dir)?;
    if conf_file.is_file() {
        println!("{} already found", conf_file.display());
        return Err(ReconError::Config(format!("{} already exists",conf_file.display())));
    }
    let conf = ReconConfig::default();
    conf.write_to_file(&conf_file)?;
    println!("created {}",conf_file.display());
    Ok(())
}


fn new(args: NewReconArgs) -> Result<(), ReconError> {
    let slurm_disabled = args.disable_slurm;
    let run_serial = args.run_serial;

    let user_input: UserInput = args.into();

    let env = Environment::get().expect("failed to resolve environment");

    println!("loaded env");

    // check if the run number has been taken
    if let Some(old_recon) = env.previous_recon(&user_input.run_number) {
        println!("this run number of found previously:");
        println!("{}", old_recon);
        println!("you wish to continue anyway?");
        if !prompt_yes_no() {
            return Err(ReconError::UserCanceled);
        }
    }

    println!("checked previous");

    // add new entry to the history log
    let new_entry = ReconHistoryEntry::try_from(user_input.clone())?;
    env.append_recon_history_entry(new_entry);

    println!("appended entry");

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
    println!("will launch recon");

    let work_dir = env.biggus.join(&user_input.project_code).join(&user_input.run_number);
    create_dir_all(&work_dir)?;

    let conf = env.recon_config(&user_input.project_code, &user_input.config_name)?;

    let mut o_conf = conf.object_config.clone();
    o_conf.remote_dir = o_conf.remote_dir.join(&user_input.raw_data_directory);
    o_conf.to_json_file(work_dir.join("obj_conf"));

    let objm:ObjectManager = o_conf.into();
    let n_objects = objm.n_objects();

    println!("n_objects: {}", n_objects);

    let scaling_object = 0;

    match conf.method {
        ReconMethod::CSCartesian { settings } => {

            let recon_settings_file = work_dir.join("reco");
            settings.to_file(&recon_settings_file);

            let slurm_dir = work_dir.join("slurm");
            create_dir_all(&slurm_dir)?;

            for i in 0..n_objects {

                let mut fetch_raw_cmd = Command::new("copy_data");
                fetch_raw_cmd.args(&[
                    work_dir.join("obj_conf").to_string_lossy().to_string(),
                    i.to_string(),
                    RequestType::Raw.to_string(),
                    work_dir.to_string_lossy().to_string(),
                    format!("raw-{i}"),
                ]);

                let mut fetch_traj_cmd = Command::new("copy_data");
                fetch_traj_cmd.args(&[
                    work_dir.join("obj_conf").to_string_lossy().to_string(),
                    i.to_string(),
                    RequestType::Trajectory.to_string(),
                    work_dir.to_string_lossy().to_string(),
                    format!("traj-{i}"),
                ]);

                let mut reco_command = Command::new("reco_cs_cartesian");
                reco_command.args(&[
                    recon_settings_file.to_string_lossy().to_string(),
                    work_dir.to_string_lossy().to_string(),
                    format!("raw-{i}"),
                    format!("traj-{i}"),
                    format!("out-{i}"),
                ]);

                let jid0 = SlurmTask::new(&slurm_dir,&format!("{}-raw-fetch-{}",&user_input.run_number,i),100)
                    .command(fetch_raw_cmd)
                    .begin_delay_sec(10 * i)
                    .submit();

                let jid1 = SlurmTask::new(&slurm_dir,&format!("{}-traj-fetch-{}",&user_input.run_number,i),100)
                    .command(fetch_traj_cmd)
                    .job_dependency_after_ok(jid0)
                    .submit();

                let jid2 = SlurmTask::new(&slurm_dir,&format!("{}-reco-{}",&user_input.run_number,i),1000)
                    .command(reco_command)
                    .job_dependency_after_ok(jid1)
                    .submit();

                if i == scaling_object {
                    let mut scale_command = Command::new("write_u16_image_scale");
                    scale_command.args(&[
                        work_dir.join(format!("out-{i}")).to_string_lossy().to_string(),
                        work_dir.join("scale-info").to_string_lossy().to_string(),
                    ]);

                    SlurmTask::new(&slurm_dir,&format!("{}-scale-info",&user_input.run_number),500)
                        .command(scale_command)
                        .job_dependency_after_ok(jid2)
                        .submit();
                }



                let rc = ReconHeadfileParams {
                    spec_id: user_input.specimen_id.clone(),
                    civmid: "".to_string(),
                    project_code: "".to_string(),
                    n_objects,
                    scanner_vendor: conf.object_config.data_host.scanner().name(),
                    run_number: user_input.run_number.clone(),
                    m_number: format!("m{}",i),
                    scale_factor_histo_percent: 0.0,
                    scale_factor_to_civmraw: 0.0,
                    scale_factor_prescale_target: 0.0,
                    scale_factor_prescale_maximum: 0.0,
                    image_code: "".to_string(),
                    image_tag: "".to_string(),
                    engine_work_dir: Default::default(),
                    more_archive_info: Default::default(),
                };

                let meta = objm.submit_meta_request(i).unwrap();
                let hf = Headfile::from_hash(&meta);
                hf.with_recon_params()

                


            }
        },
        ReconMethod::FFT => {todo!()}
    }
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