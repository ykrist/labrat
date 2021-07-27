use std::time::Duration;
use std::io::{BufReader};
use std::fs::File;
use std::os::unix::io::{FromRawFd, RawFd};
use std::process::exit;
use structopt::*;
use anyhow::{Result, Context};
use sha2::Digest;
use std::path::{PathBuf, Path};
use serde::de::DeserializeOwned;

pub use serde::{Serialize, Deserialize};
pub use structopt::StructOpt;
use structopt::StructOptInternal;

#[derive(Debug, Copy, Clone, StructOpt, Default)]
pub struct NoAuxParams {}

pub trait NewOutput: Sized + Serialize + DeserializeOwned {
    type Inputs;
    type Params;
    type AuxParams;

    fn new(inputs: &Self::Inputs, params: &Self::Params, aux: &Self::AuxParams) -> Self;
}

pub trait Experiment: Sized {
    type Inputs: StructOptInternal + Serialize + DeserializeOwned + IdStr;
    type Parameters: StructOptInternal + Serialize + DeserializeOwned + IdStr;
    type AuxParameters: StructOptInternal + Default;
    type Outputs: NewOutput<Inputs=Self::Inputs, Params=Self::Parameters, AuxParams=Self::AuxParameters>;

    fn inputs(&self) -> &Self::Inputs;
    fn outputs(&self) -> &Self::Outputs;
    fn parameters(&self) -> &Self::Parameters;

    fn log_root_dir() -> PathBuf;

    fn new(inputs: Self::Inputs, parameters: Self::Parameters, aux_parameters: Self::AuxParameters, outputs: Self::Outputs) -> Self;

    fn post_parse(_inputs: &Self::Inputs, _parameters: &mut Self::Parameters) {}

    fn get_output_path(&self, filename: &str) -> PathBuf {
        let mut log_dir = Self::log_root_dir();
        log_dir.push(self.parameters().id_str());
        let mut log_dir = ensure_directory_exists(log_dir).unwrap();
        log_dir.push(filename);
        log_dir
    }

    fn get_output_path_prefixed(&self, filename: &str) -> PathBuf {
        let mut log_dir = Self::log_root_dir();
        log_dir.push(self.parameters().id_str());
        let mut log_dir = ensure_directory_exists(log_dir).unwrap();
        log_dir.push(format!("{}-{}", self.inputs().id_str(), filename));
        log_dir
    }

    fn write_index_file(&self) -> Result<()> {
        let p = self.get_output_path(&format!("{}-index.json", self.inputs().id_str()));
        let contents = serde_json::json!({
            "input": self.inputs(),
            "output" : self.outputs(),
        });
        let contents = serde_json::to_string_pretty(&contents)?;
        std::fs::write(p, contents)?;
        Ok(())
    }

    fn write_parameter_file(&self) -> Result<()> {
        let p = self.get_output_path("parameters.json");
        if !p.exists() {
            std::fs::write(p, serde_json::to_string_pretty(self.parameters())?)?;
        }
        Ok(())
    }

    fn from_index_file(path: impl AsRef<Path>) -> Result<Self> {
        #[derive(Debug, Clone, Deserialize)]
        struct Index<I, O> {
            input: I,
            output: O,
        }

        let r = BufReader::new(
            File::open(&path).with_context(|| format!("failed to read {:?}", path.as_ref()))?
        );
        let index:  Index<Self::Inputs, Self::Outputs> = serde_json::from_reader(r)?;
        let Index{ input, output } = index;

        let param_file =path.as_ref().with_file_name("parameters.json");


        let r = BufReader::new(
            File::open(&param_file).with_context(|| format!("failed to read {:?}", param_file))?
        );
        let params : Self::Parameters = serde_json::from_reader(r)?;
        Ok(Self::new(input, params, Default::default(), output))
    }
}

pub fn id_from_serialised<T: Serialize + ?Sized>(val: &T) -> String {
    let mut hasher = sha2::Sha224::new();
    hasher.update(&serde_json::to_string(val).unwrap());
    base_62::encode(hasher.finalize().as_slice())
}


fn ensure_directory_exists(path: impl AsRef<Path>) -> Result<PathBuf> {
    match std::fs::create_dir_all(path.as_ref()) {
        Ok(()) => {}
        Err(e) => match e.kind() {
            std::io::ErrorKind::AlreadyExists => {},
            _ => return Err(e.into()),
        }
    };
    return Ok(path.as_ref().canonicalize().unwrap())
}

pub trait IdStr: Serialize {
    fn id_str(&self) -> String {
        id_from_serialised(self)
    }
}

pub struct MemoryAmount(usize);

impl MemoryAmount {
    pub fn from_mb(amount: usize) -> Self { MemoryAmount(amount) }

    pub fn from_gb(amount: usize) -> Self { MemoryAmount(amount * 1000) }

    pub fn from_gb_f64(amount: f64) -> Self {
        MemoryAmount((amount * 1000.0).round() as usize)
    }

    pub fn as_mb(&self) -> usize { self.0 }
}


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
        }.to_owned()
    }
}


#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SlurmResources {
    script: String,
    #[serde(rename="err")]
    log_err: PathBuf,
    #[serde(rename="out")]
    log_out: PathBuf,
    #[serde(skip_serializing_if="Option::is_none")]
    job_name: Option<String>,
    cpus: usize,
    nodes: usize,
    time: String,
    memory: String,
    #[serde(skip_serializing_if="Option::is_none")]
    mail_user: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    mail_type: Option<String>,
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
    pub fn get(exp: &impl ResourcePolicy) -> Self {
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
            mail_type,
            cpus: exp.cpus(),
            nodes: exp.nodes(),
        }
    }
}


pub trait ResourcePolicy: Experiment {
    fn script(&self) -> String;
    fn time(&self) -> Duration;
    fn memory(&self) -> MemoryAmount;
    fn cpus(&self) -> usize { 1 }
    fn nodes(&self) -> usize { 1 }
    fn job_name(&self) -> Option<String> {
        Some(self.parameters().id_str())
    }
    fn mail_user(&self) -> Option<String> { None }
    fn mail_type(&self) -> Vec<MailType> { Vec::new() }
    fn log_err(&self) -> PathBuf {
        self.get_output_path(&format!("{}.err", self.inputs().id_str()))
    }
    fn log_out(&self) -> PathBuf {
        self.get_output_path(&format!("{}.out", self.inputs().id_str()))
    }
}


pub trait ArgEnum: std::str::FromStr {
    fn choices() -> &'static [&'static str];
}

#[macro_export]
macro_rules! impl_arg_enum {
    ($ty:path;
      $($variant:ident = $string:literal),+ $(,)?
    ) => {
      impl ::slurm_harray::ArgEnum for $ty {
        fn choices() -> &'static [&'static str] {
          &[
            $($string),*
          ]
        }
      }

      impl ::std::str::FromStr for $ty {
        type Err = String;
        fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
          let v = match s {
            $($string => <$ty>::$variant,)*
            _ => return Err(format!("failed to parse {} to {}", s, stringify!($ty))),
          };
          Ok(v)
        }
      }
    };
}


#[macro_export]
macro_rules! impl_experiment_helper {
    (
        $input:ident : $input_ty:path ;
        $param:ident : $param_ty:path;
        $output:ident : $output_ty:path;
        $($aux_param:ident : $aux_param_ty:path;)?
    ) => {
            type Inputs = $input_ty;
            type Parameters = $param_ty;
            impl_experiment_helper!{ @AUX_PARAM_ATYPE $($aux_param_ty)* }
            type Outputs = $output_ty;

            fn new(inputs: Self::Inputs, params: Self::Parameters, aux_params: Self::AuxParameters, outputs: Self::Outputs) -> Self {
               Self {
                   $input: inputs,
                   $param: params,
                   $($aux_param: aux_params,)*
                   $output: outputs,
               }
            }

            fn outputs(&self) -> &Self::Outputs {
                &self.$output
            }

            fn inputs(&self) -> &Self::Inputs {
               &self.$input
            }

            fn parameters(&self) -> &Self::Parameters {
                &self.$param
            }
    };

    (@AUX_PARAM_ATYPE ) => {
        type AuxParameters = $crate::NoAuxParams;
    };

    (@AUX_PARAM_ATYPE $aux_param_ty:path) => {
        type AuxParameters = $aux_param_ty;
    };
}

#[derive(StructOpt, Debug, Clone)]
struct SlurmArgs {
    /// Start the Slurm info pipe server with file descriptors R (Reading) and W (Writing)
    #[structopt(
        long="p-slurminfo",
        number_of_values=2,
        value_names=&["R", "W"],
    )]
    pipe: Option<Vec<RawFd>>,
    /// Print Slurm info as a JSON string and exit.
    #[structopt(long="slurminfo")]
    info: bool,
}

#[derive(StructOpt, Debug, Clone)]
struct NoSlurmArgs {}

#[derive(StructOpt, Debug, Clone)]
struct ClArgs<S: StructOpt, T: Experiment> {
    #[structopt(flatten)]
    slurm: S,
    #[structopt(flatten)]
    inputs: T::Inputs,
    #[structopt(flatten)]
    parameters: T::Parameters,
    #[structopt(flatten)]
    aux_parameters: T::AuxParameters,
    #[structopt(long, short="l", value_name="json file")]
    /// Load parameters from file.  All other parameter arguments will be ignored.
    load_params: Option<PathBuf>
}


impl<S: StructOpt, T: Experiment> ClArgs<S, T> {
    fn into_experiment(self) -> Result<T>
    {
        let ClArgs{ slurm: _, inputs,mut parameters, aux_parameters, load_params } = self;
        if let Some(p) = load_params {
            let s = std::fs::read_to_string(p).context("failed to read parameters from file")?;
            parameters = serde_json::from_str(&s).context("failed to deserialise parameters")?;
        }
        T::post_parse(&inputs, &mut parameters);
        let outputs = T::Outputs::new(&inputs, &parameters, &aux_parameters);
        Ok(T::new(inputs, parameters, aux_parameters, outputs))
    }
}


fn run_pipe_server<T>(read_fd: RawFd, write_fd: RawFd) -> Result<()>
    where
      T: ResourcePolicy,
{
    let reader: File = unsafe { File::from_raw_fd(read_fd as RawFd) };
    let writer: File = unsafe { File::from_raw_fd(write_fd as RawFd) };

    let commands : Vec<Vec<String>> = serde_json::from_reader(reader)?;
    let mut slurm_job_specs = Vec::new();
    let mut app = ClArgs::<NoSlurmArgs, T>::clap();

    for cmd in commands {
        let matches= app.get_matches_from_safe_borrow(&cmd)?;
        let args = ClArgs::<NoSlurmArgs, T>::from_clap(&matches);
        let exp: T = args.into_experiment()?;
        slurm_job_specs.push(SlurmResources::get(&exp))
    }

    serde_json::to_writer(writer, &slurm_job_specs)?;
    Ok(())
}

fn apply_clap_settings<'a, 'b>(app: clap::App<'a, 'b>) -> clap::App<'a, 'b> {
    app
      .group(clap::ArgGroup::with_name("slurm-managed")
          .args(&["pipe", "info"])
      )
      .setting(clap::AppSettings::NextLineHelp)
      .setting(clap::AppSettings::ColoredHelp)
      .name("")
}

fn check_args_for_slurm_pipe() -> Result<Option<(RawFd, RawFd)>> {
    fn parse_fd(arg: &Option<String>) -> Result<RawFd> {
        let fd = arg.as_ref().ok_or_else(|| anyhow::anyhow!("--p-slurminfo takes two integer arguments."))?;
        fd.parse().with_context(|| format!("Failed to parse file descriptor `{}`", &fd))
    }

    let mut args = std::env::args();

    let mut pipe_slurminfo_found = false;
    let mut rd = None;
    let mut wd = None;

    while let Some(s) = args.next() {
        if s == "--p-slurminfo" {
            pipe_slurminfo_found = true;
            rd = args.next();
            wd = args.next();
        } else if s == "--help" || s == "-h" {
            return Ok(None)
        }
    }

    if pipe_slurminfo_found {
        let rd = parse_fd(&rd)?;
        let wr = parse_fd(&wd)?;
        return Ok(Some((rd, wr)))
    }
    Ok(None)
}

pub fn handle_slurm_args<T>() -> Result<T>
    where
        T: ResourcePolicy,
{
    if let Some((read_fd, write_fd)) = check_args_for_slurm_pipe()? {
        run_pipe_server::<T>(read_fd, write_fd)?;
        exit(0)
    }

    let app = apply_clap_settings(ClArgs::<SlurmArgs, T>::clap());
    let args = ClArgs::<SlurmArgs, T>::from_clap(&app.get_matches());

    let slurm_info = args.slurm.info;
    let exp: T = args.into_experiment()?;

    if slurm_info {
        println!("{}", serde_json::to_string_pretty(&SlurmResources::get(&exp))?);
        exit(0);
    }

    Ok(exp)
}

