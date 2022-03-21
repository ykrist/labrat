use anyhow::{Context, Result};
use clap::Parser;
use serde::de::DeserializeOwned;
use sha2::Digest;
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufReader, stdout};
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::process::exit;

pub use clap::{ArgEnum, Args};
pub use serde::{Deserialize, Serialize};

fn read_json<T, P>(path: P) -> Result<T>
    where
        T: DeserializeOwned,
        P: AsRef<Path> + Debug,
{
    let file = File::open(&path)
        .map(BufReader::new)
        .with_context(|| format!("unable to read {:?}", &path))?;
    
    let x: T = serde_json::from_reader(file)?;
    
    Ok(x)
}

/// A marker type used when there is no Config.
#[derive(Debug, Copy, Clone, clap::Args, Default)]
pub struct NoConfig;

/// The main trait.  A type which implements experiment describes 4 classes of values:
/// - **Inputs** These are the inputs to the experiment.  These are var
/// - **Parameters** These are the inputs which the experiment is trying to test the effects of.  
/// - **Output** The results of experiment.  Typically, this is a struct whose fields are filenames (without the directory)
///     The filenames refer to files placed in the output directory.
/// - **Config** These are parameters that control *what* is output, but not how the experiment runs.  For example, part of a 
///     Config struct might be a flag which controls whether an output file is present or not.
/// 
/// Experiments are associated with a directory structure: `ROOT/PARAM_ID/` where `PARAM_ID` is the string produced by 
/// `Self::Parameters::id_str()` (see [`IdStr`]). `ROOT` is the directory produced by `root_dir()`. 
/// 
pub trait Experiment: Sized
{
    type Input: Args + Serialize + DeserializeOwned + IdStr;
    type Parameters: Args + Serialize + DeserializeOwned + IdStr;
    type Config: Args + Default;
    type Output: Serialize + DeserializeOwned;

    /// Experiment inputs
    fn input(&self) -> &Self::Input;

    /// Experiment outputs
    fn output(&self) -> &Self::Output;

    /// Experiment parameters
    fn parameter(&self) -> &Self::Parameters;

    /// Construct a new experiment from its parts
    fn new(
        prof: Profile,
        config: Self::Config,
        inputs: Self::Input,
        parameters: Self::Parameters,
        outputs: Self::Output,
    ) -> Self;

    /// Derive the output from input, parameters and config.  This is not included in [`Experiment::new()`], since
    /// and experiment constructed from parts may be read from parts.
    fn new_output(inputs: &Self::Input, params: &Self::Parameters, config: &Self::Config) -> Self::Output;

    /// The root directory for outputs
    fn root_dir() -> PathBuf;

    /// A hook for modifying parameters and config after parsing from command-line arguments.
    fn post_parse(
        _prof: Profile,
        _inputs: &Self::Input,
        _parameters: &mut Self::Parameters,
        _config: &mut Self::Config,
    ) {
    }

    /// Given a base filename, return the full path to where the file should be placed.  
    /// 
    /// Eg, for `filename`, returns `ROOT/PARAM_ID/filename`
    fn get_output_path(&self, filename: &str) -> PathBuf {
        let mut log_dir = Self::root_dir();
        log_dir.push(self.parameter().id_str());
        let mut log_dir = ensure_directory_exists(log_dir).unwrap();
        log_dir.push(filename);
        log_dir
    }


    /// Given a base filename, return the full path to where the file should be placed.  The filename 
    /// is first prefixed with `self.input().id_str()`.
    /// 
    /// Eg, if `filename` is `-hello.txt`, returns `ROOT/PARAM_ID/INPUT_ID-hello.txt`
    fn get_output_path_prefixed(&self, filename: &str) -> PathBuf {
        let mut log_dir = Self::root_dir();
        log_dir.push(self.parameter().id_str());
        let mut log_dir = ensure_directory_exists(log_dir).unwrap();
        log_dir.push(format!("{}{}", self.input().id_str(), filename));
        log_dir
    }

    /// Write the index file to the output directory.
    fn write_index_file(&self) -> Result<()> {
        let p = self.get_output_path_prefixed("-index.json");
        let contents = serde_json::json!({
            "input": self.input(),
            "output" : self.output(),
        });
        let contents = serde_json::to_string_pretty(&contents)?;
        std::fs::write(p, contents)?;
        Ok(())
    }

    /// Write the parameter file to the output directory.
    fn write_parameter_file(&self) -> Result<()> {
        let p = self.get_output_path("parameters.json");
        if !p.exists() {
            std::fs::write(p, serde_json::to_string_pretty(self.parameter())?)?;
        }
        Ok(())
    }

    /// Instantiate an experiment from disk
    fn from_index_file(path: impl AsRef<Path> + Debug) -> Result<Self> {
        #[derive(Debug, Clone, Deserialize)]
        struct Index<I, O> {
            input: I,
            output: O,
        }

        let index: Index<Self::Input, Self::Output> = read_json(&path)?;
        let Index { input, output } = index;

        let param_file = path.as_ref().with_file_name("parameters.json");
        let params: Self::Parameters = read_json(param_file)?;
        Ok(Self::new(
            Profile::Default,
            Default::default(),
            input,
            params,
            output,
        ))
    }

    /// Construct a new experiment from command-line arguments.
    fn from_cl_args() -> Result<Self> {
        ClArgs::<NoSlurmArgs, Self>::parse().into_experiment()
    }
}

/// A helper function for quickly implementing [`IdStr`] for types 
/// which are [`Serialize`].  Note this may produce collisions, but it is
/// extremely unlikely.
pub fn id_from_serialised<T: Serialize + ?Sized>(val: &T) -> String {
    let mut hasher = sha2::Sha224::new();
    hasher.update(&serde_json::to_string(val).unwrap());
    base_62::encode(hasher.finalize().as_slice())
}

fn ensure_directory_exists(path: impl AsRef<Path>) -> Result<PathBuf> {
    match std::fs::create_dir_all(path.as_ref()) {
        Ok(()) => {}
        Err(e) => match e.kind() {
            std::io::ErrorKind::AlreadyExists => {}
            _ => return Err(e.into()),
        },
    };
    return Ok(path.as_ref().canonicalize().unwrap());
}


/// Has a filename-friendly string ID.
pub trait IdStr: Serialize {
    fn id_str(&self) -> String {
        id_from_serialised(self)
    }
}

/// An amount of memory for Slurm.
pub struct MemoryAmount(usize);

impl MemoryAmount {
    pub fn from_mb(amount: usize) -> Self {
        MemoryAmount(amount)
    }

    pub fn from_gb(amount: usize) -> Self {
        MemoryAmount(amount * 1000)
    }

    pub fn from_gb_f64(amount: f64) -> Self {
        MemoryAmount((amount * 1000.0).round() as usize)
    }

    pub fn as_mb(&self) -> usize {
        self.0
    }
}

/// Slurm email notification events. See the `--mail-type` parameter to [`sbatch`](https://slurm.schedmd.com/sbatch.html)
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MailType {
    None,
    Begin,
    End,
    Fail,
    Requeue,
    All,
    InvalidDepend,
    StageOut,
    TimeLimit,
    TimeLimit90,
    TimeLimit80,
    TimeLimit50,
    ArrayTasks,
}

impl ToString for MailType {
    fn to_string(&self) -> String {
        use MailType::*;
        match self {
            None => "NONE",
            Begin => "BEGIN",
            End => "END",
            Fail => "FAIL",
            Requeue => "REQUEUE",
            All => "ALL",
            InvalidDepend => "INVALID_DEPEND",
            StageOut => "STAGE_OUT",
            TimeLimit => "TIME_LIMIT",
            TimeLimit90 => "TIME_LIMIT_90",
            TimeLimit80 => "TIME_LIMIT_80",
            TimeLimit50 => "TIME_LIMIT_50",
            ArrayTasks => "ARRAY_TASKS",
        }
        .to_owned()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SlurmResources {
    #[serde(rename = "script")]
    script: String,
    #[serde(rename = "err")]
    log_err: PathBuf,
    #[serde(rename = "out")]
    log_out: PathBuf,
    #[serde(rename = "job-name", skip_serializing_if = "Option::is_none")]
    job_name: Option<String>,
    #[serde(rename = "cpus-per-task")]
    cpus: usize,
    #[serde(rename = "nodes")]
    nodes: usize,
    #[serde(rename = "time")]
    time: String,
    #[serde(rename = "mem")]
    memory: String,
    #[serde(rename = "mail-user", skip_serializing_if = "Option::is_none")]
    mail_user: Option<String>,
    #[serde(rename = "mail-type", skip_serializing_if = "Option::is_none")]
    mail_type: Option<String>,
    #[serde(rename = "constraint", skip_serializing_if = "Option::is_none")]
    constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exclude: Option<String>,
    #[serde(rename = "constraint", skip_serializing_if = "Option::is_none")]
    nodelist: Option<String>,
}

fn fmt_as_slurm_time(mut secs: u64) -> String {
    let mut minutes = secs / 60;
    secs -= minutes * 60;
    let mut hrs = minutes / 60;
    minutes -= hrs * 60;
    let days = hrs / 24;
    hrs -= days * 24;
    format!("{}-{}:{:02}:{:02}", days, hrs, minutes, secs)
}

impl SlurmResources {
    pub fn new(exp: &impl ResourcePolicy) -> Self {
        let mail_type = {
            let mt = exp.mail_type();
            if mt.is_empty() {
                None
            } else {
                let mt: Vec<_> = mt.into_iter().map(|m| m.to_string()).collect();
                Some(mt.join(","))
            }
        };

        SlurmResources {
            time: fmt_as_slurm_time(exp.time().as_secs()),
            memory: format!("{}MB", exp.memory().as_mb()),
            script: exp.script(),
            log_err: exp.log_err(),
            log_out: exp.log_out(),
            job_name: exp.job_name(),
            mail_user: exp.mail_user(),
            constraint: exp.constraint(),
            mail_type,
            cpus: exp.cpus(),
            nodes: exp.nodes(),
            nodelist: exp.nodelist(),
            exclude: exp.exclude(),
        }
    }
}

/// For running with `slurm-harray`, your main experiment should implement this trait,
/// which gives you access to the [`ResourcePolicy::from_cl_args_with_slurm`] constructor.
pub trait ResourcePolicy: Experiment {
    /// The Slurm script loaded as a string.
    fn script(&self) -> String;
    
    /// Time limit for this job
    fn time(&self) -> Duration;

    /// Maximum amount of memory allocated to this job
    fn memory(&self) -> MemoryAmount;

    /// Number of CPUs
    fn cpus(&self) -> usize { 1 }

    /// Number of compute nodes
    fn nodes(&self) -> usize { 1 }

    /// The job name.  The default is the parameter ID-string.
    fn job_name(&self) -> Option<String> {
        Some(self.parameter().id_str())
    }

    /// Email to send notifications to
    fn mail_user(&self) -> Option<String> {
        None
    }

    /// Which notifications to subscribe to.  Default is none.
    fn mail_type(&self) -> Vec<MailType> {
        Vec::new()
    }

    /// Slurm allocation constraints (`sbatch --constraint`)
    fn constraint(&self) -> Option<String> {
        None
    }

    /// Exclude certain nodes (`sbatch --exclude`)
    fn exclude(&self) -> Option<String> {
        None
    }

    /// Specify a nodelist (`sbatch --nodelist`)
    fn nodelist(&self) -> Option<String> {
        None
    }

    /// Path to place STDERR log. Should be an absolute path.  [`Experiment::get_output_path`] or 
    /// [`Experiment::get_output_path_prefixed`] may be helpful.
    fn log_err(&self) -> PathBuf {
        self.get_output_path_prefixed(".err")
    }

    /// Path to place STDERR log. Should be an absolute path.  [`Experiment::get_output_path`] or 
    /// [`Experiment::get_output_path_prefixed`] may be helpful.
    fn log_out(&self) -> PathBuf {
        self.get_output_path_prefixed(".out")
    }

    /// Parse command-line arguments for inputs, parameters and config, before handling 
    /// and Slurm-related arguments.  May exit the program.
    fn from_cl_args_with_slurm() -> Result<Self> {
        if let Some((read_fd, write_fd)) = check_args_for_slurm_pipe()? {
            run_pipe_server::<Self>(read_fd, write_fd)?;
            exit(0)
        }
    
        let args = ClArgs::<SlurmArgs, Self>::parse();
        let slurm_info = args.slurm.info;
        let exp = args.into_experiment()?;
    
        if slurm_info {
            serde_json::to_writer_pretty(stdout(), &SlurmResources::new(&exp))?;
            exit(0);
        }
    
        Ok(exp)
    }
}

#[derive(clap::Args, Debug, Clone)]
struct SlurmArgs {
    /// Start the Slurm info pipe server with file descriptors R (Reading) and W (Writing)
    /// All other arguments are ignored.
    #[allow(dead_code)]
    #[clap(
        long="p-slurminfo",
        number_of_values=2,
        value_names=&["R", "W"],
        group("slurm-managed"),
    )]
    pipe: Option<Vec<RawFd>>,
    /// Print Slurm info as a JSON string and exit.
    #[clap(long = "slurminfo", group("slurm-managed"))]
    info: bool,
}

#[derive(clap::Args, Debug, Clone)]
struct NoSlurmArgs {}

#[derive(clap::Parser, Debug, Clone)]
#[clap(setting(clap::AppSettings::DeriveDisplayOrder))]
#[clap(next_line_help(true))]
#[clap(bin_name="")]
struct ClArgs<S: clap::Args, T: Experiment> {
    /// Which profile to use.  Different profiles allow you to request
    /// more resources for debugging runs or enable, for example.
    #[clap(arg_enum, long = "profile", alias="slurmprofile", default_value_t)]
    profile: Profile,
    #[clap(flatten, next_help_heading="Slurm-Managed")]
    slurm: S,
    #[clap(flatten, next_help_heading = "Input")]
    inputs: T::Input,
    #[clap(flatten, next_help_heading = "Parameters")]
    parameters: T::Parameters,
    #[clap(flatten, next_help_heading = "Config")]
    config: T::Config,
    #[clap(long, short = 'l', value_name = "json file", help_heading="Parameters")]
    /// Load parameters from file.  All other parameter arguments will be ignored.
    load_params: Option<PathBuf>,
}

/// Experiment profile.  Different profiles allow experiments to be debugged and tested easier.  
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, ArgEnum)]
pub enum Profile {
    Default,
    Test,
    Trace,
}

impl Default for Profile {
    fn default() -> Self {
        Profile::Default
    }
}

impl<S: clap::Args, T: Experiment> ClArgs<S, T> {
    fn into_experiment(self) -> Result<T> {
        let ClArgs {
            slurm: _,
            profile,
            inputs,
            mut parameters,
            mut config,
            load_params,
        } = self;
        if let Some(p) = load_params {
            parameters = read_json(p).context("failed to deserialise parameters")?;
        }
        T::post_parse(profile, &inputs, &mut parameters, &mut config);
        let outputs = T::new_output(&inputs, &parameters, &config);
        Ok(T::new(profile, config, inputs, parameters, outputs))
    }
}

fn run_pipe_server<T>(read_fd: RawFd, write_fd: RawFd) -> Result<()>
where
    T: ResourcePolicy,
{
    let reader: File = unsafe { File::from_raw_fd(read_fd as RawFd) };
    let writer: File = unsafe { File::from_raw_fd(write_fd as RawFd) };

    let commands: Vec<Vec<String>> = serde_json::from_reader(reader)?;
    let mut slurm_job_specs = Vec::new();

    for cmd in commands { // cmd is expected to have an argv[0] which is ignored.
        let args = ClArgs::<NoSlurmArgs, T>::try_parse_from(cmd)?;
        let exp: T = args.into_experiment()?;
        slurm_job_specs.push(SlurmResources::new(&exp))
    }

    serde_json::to_writer(writer, &slurm_job_specs)?;
    Ok(())
}

fn check_args_for_slurm_pipe() -> Result<Option<(RawFd, RawFd)>> {
    fn parse_fd(arg: &Option<String>) -> Result<RawFd> {
        let fd = arg
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--p-slurminfo takes two integer arguments."))?;
        fd.parse()
            .with_context(|| format!("Failed to parse file descriptor `{}`", &fd))
    }

    let mut args = std::env::args();

    let mut pipe_slurminfo_found = false;
    let mut rd = None;
    let mut wd = None;

    while let Some(s) = args.next() {
        if s == "--p-slurminfo" {
            if pipe_slurminfo_found { anyhow::bail!("--p-slurminfo supplied multiple times") }
            pipe_slurminfo_found = true;
            rd = args.next();
            wd = args.next();
        } else if s == "--help" || s == "-h" {
            return Ok(None);
        }
    }

    if pipe_slurminfo_found {
        let rd = parse_fd(&rd)?;
        let wr = parse_fd(&wd)?;
        return Ok(Some((rd, wr)));
    }
    Ok(None)
}
