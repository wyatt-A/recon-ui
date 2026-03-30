use std::fmt::Display;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::process::Command;
use array_lib::io_cfl::{read_cfl, write_cfl};
use civm_raw::{u16_scale_from_cf32, ImageScale};
use civm_raw::raw::write_magnitude;
use clap::Parser;
use headfile::common::{DWHeadfileParams, ReconHeadfileParams};
use headfile::Headfile;
use object_manager::object::ObjectManager;
use object_manager::{JsonState, TomlConf};
use object_manager::request::RequestType;
use recon_lib::{run_cs_cartesian, ReconMethod};
use slurm_interface::{JobState, SlurmTask};
use recon_ui::config::{ReconConfig, TomlConfig, UserInput, UserProfile};
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
    let user_profile = env.user_profile(&user_input.project_code).unwrap();

    let archive_info = env.archive_params(&user_input.project_code).unwrap();

    let mut o_conf = conf.object_config.clone();
    o_conf.remote_dir = o_conf.remote_dir.join(&user_input.raw_data_directory);
    o_conf.to_json_file(work_dir.join("obj_conf"));

    let objm:ObjectManager = o_conf.into();
    let n_objects = objm.n_objects();
    let n_dig = n_objects.to_string().chars().count();

    println!("n_objects: {}", n_objects);

    let scaling_object = 0;

    match conf.method {
        ReconMethod::CSCartesian { settings } => {

            let recon_settings_file = work_dir.join("reco");
            settings.to_file(&recon_settings_file);



            for i in 0..n_objects {


                let (raw,raw_dims) = objm.submit_raw_request(i).unwrap();
                let (traj,traj_dims) = objm.submit_traj_request(i).unwrap();

                let raw_file = format!("raw-{i}");
                let traj_file = format!("traj-{i}");
                let out_file = format!("out-{i}");

                write_cfl(work_dir.join(&raw_file),&raw,raw_dims);
                write_cfl(work_dir.join(&traj_file),&traj,traj_dims);
                run_cs_cartesian(&settings,&work_dir,work_dir.join(&raw_file),work_dir.join(&traj_file),work_dir.join(&out_file));

                if i == scaling_object {
                    let scaling_img = format!("out-{i}");
                    let (data,_) = read_cfl(work_dir.join(scaling_img));
                    let scale = u16_scale_from_cf32(&data,conf.scale_undersaturation_fraction.unwrap_or(0.9995));
                    println!("found scale");
                    scale.to_file(work_dir.join("scale-info")).unwrap();
                }

                let scale_info = ImageScale::from_file(work_dir.join("scale-info")).unwrap();

                let rc = ReconHeadfileParams {
                    spec_id: user_input.specimen_id.clone(),
                    civmid: user_profile.username.clone(),
                    project_code: user_input.project_code.clone(),
                    n_objects,
                    scanner_vendor: conf.object_config.data_host.scanner().vendor(),
                    run_number: user_input.run_number.clone(),
                    m_number: format!("m{:0width$}",i,width=n_dig),
                    scale_factor_histo_percent: scale_info.histogram_percent,
                    scale_factor_to_civmraw: scale_info.scale_factor,
                    scale_factor_prescale_target: scale_info.pre_scale_target,
                    scale_factor_prescale_maximum: scale_info.pre_scale_max,
                    image_code: conf.object_config.data_host.scanner().image_code(),
                    image_tag: "imx".to_string(),
                    engine_work_dir: work_dir.clone(),
                    more_archive_info: archive_info.clone(),
                };

                let meta = objm.submit_meta_request(i).unwrap();
                let mut hf = Headfile::from_hash(&meta);

                hf.insert_toml_table(&settings.to_toml_table(),true);

                if let Some((_,_,bvals)) = hf.get_numeric_vector("b_trace") {
                    let bvalue = *bvals.get(i).unwrap() as f32;
                    if let Some((_,_,bvecs)) = hf.get_numeric_vector("bvecs") {
                        let vecs:Vec<_> = bvecs.chunks_exact(3).collect();
                        let bvec = vecs.get(i).unwrap();
                        let bv = bvec.iter().map(|x| *x as f32).collect::<Vec<f32>>();
                        let dw = DWHeadfileParams {
                            bvalue,
                            bval_dir: [bv[0],bv[1],bv[2]],
                        };
                        hf = hf.with_diffusion_params(dw);
                    }
                }
                hf = hf.with_recon_params(rc);
                println!("added recon params");
                let (data,dims) = read_cfl(work_dir.join(out_file));
                write_magnitude(&work_dir,hf,&data,dims).unwrap();

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