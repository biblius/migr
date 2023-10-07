use crate::migration::migration_generate;
use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use migration::{migration_redo, migration_rev, migration_run, setup, status, sync};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{env, path::PathBuf};

mod migration;

pub static VERBOSE: AtomicBool = AtomicBool::new(false);

fn main() -> anyhow::Result<()> {
    let migr = Migr::parse();

    if migr.verbose {
        VERBOSE.fetch_or(true, Ordering::AcqRel);
    }

    let url = env::var("DATABASE_URL")
        .context("`DATABASE_URL` must be set in the env before running migr")?;

    let mut pg = establish_connection(&url);

    match migr.command {
        MigrationSubcommand::Status => status(&mut pg),
        MigrationSubcommand::Setup => {
            let path = format!("{}/migrations", migr.path.as_deref().unwrap_or("."));
            setup(path.into(), &mut pg)
        }
        MigrationSubcommand::Sync(ref args) => {
            let path = path(&migr)?;
            sync(args.trim, &path, &mut pg)
        }
        MigrationSubcommand::Gen(ref args) => {
            let path = path(&migr)?;
            migration_generate(args, path, pg)
        }
        MigrationSubcommand::Run(ref args) => {
            let path = path(&migr)?;
            migration_run(args, path, pg)
        }
        MigrationSubcommand::Rev(ref args) => {
            let path = path(&migr)?;
            migration_rev(args, path, pg)
        }
        MigrationSubcommand::Redo(ref args) => {
            let path = path(&migr)?;
            migration_redo(args, path, pg)
        }
    }
}

fn establish_connection(url: &str) -> postgres::Client {
    postgres::Client::connect(url, postgres::NoTls).expect("Could not establish PG connection")
}

fn path(migr: &Migr) -> anyhow::Result<PathBuf> {
    let path = migr.path.as_ref().map(PathBuf::from);
    if let Some(path) = path {
        return Ok(path);
    }
    let current_dir = env::current_dir()?;
    find_migrations(current_dir, 0, migr.depth)?
        .ok_or(anyhow::Error::msg("Unable to locate migrations directory"))
}

#[derive(Debug, Parser)]
#[command(name = "migr", author = "biblius", version = "0.1", about = "Minimal PG migration tool", long_about = None)]
pub struct Migr {
    #[clap(subcommand)]
    pub command: MigrationSubcommand,

    /// If provided, migr will setup/load migrations in the given directory.
    #[arg(long, short)]
    path: Option<String>,

    /// If a path is not provided, migr will search for a 'migrations' directory `depth` levels deep from the current one.
    #[arg(long, short, default_value = "2")]
    depth: usize,

    /// Print migr plumbing to stdout.
    #[arg(long, short, action)]
    verbose: bool,
}

#[derive(Debug, Subcommand)]
pub enum MigrationSubcommand {
    /// Show the state of migrations in the metadata table.
    Status,
    /// Initialise a migration directory, set up the initial migration and create the metadata table.
    Setup,
    /// Sync existing/edited migrations with migr.
    Sync(SyncArgs),
    /// Generate a new migration
    Gen(GenMigration),
    /// Run pending migrations
    Run(RunRevMigration),
    /// Reverse migrations
    Rev(RunRevMigration),
    /// Redo migrations
    Redo(RunRevMigration),
}

#[derive(Debug, Args, Default, Clone)]
pub struct SyncArgs {
    #[arg(long, short, action)]
    /// Diffs the migrations directory with entries from the metadata table and removes all
    /// table entries that do not exist in the directory.
    trim: bool,
}

#[derive(Debug, Args, Default, Clone)]
pub struct GenMigration {
    /// Migration name
    pub name: String,
}

#[derive(Debug, Args, Default, Clone)]
pub struct RunRevMigration {
    /// The exact migration to perform the action on. This will disregard the entry in the metadata table and will also update it.
    #[arg(long, short)]
    pub exact: Option<String>,

    /// The number of migrations to run/revert/redo. Defaults to `1` when reverting.
    #[arg(long, short)]
    pub count: Option<usize>,

    /// If true, performs the action on all migrations. Defaults to `true` when running.
    #[arg(long, short, action)]
    pub all: bool,
}

/// Gets the path of the directory where migrations are located. Skips `target` and any directories starting
/// with `.`.
fn find_migrations(
    path: PathBuf,
    depth: usize,
    max_depth: usize,
) -> Result<Option<PathBuf>, std::io::Error> {
    if depth > max_depth {
        return Ok(None);
    }

    // Try to find the migrations in root as usually that's where they're placed
    if depth == 0 && path.is_dir() {
        info!(
            "Searching for migrations in {}",
            path.display().to_string().purple()
        );
        for entry in path.read_dir()? {
            let entry = entry?;
            if entry.file_name() == "migrations" {
                let path = entry.path();
                info!(
                    "Found migrations at {}",
                    path.display().to_string().purple(),
                );
                return Ok(Some(path));
            }
        }
    }

    trace!("in {}", path.display().to_string().as_str().blue());

    for entry in path.read_dir()? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_dir()
            || entry.file_name() == "target"
            || entry
                .file_name()
                .to_str()
                .is_some_and(|s| s.starts_with('.'))
        {
            continue;
        }

        if entry.file_name() == "migrations" {
            let path = entry.path();
            info!(
                "Found migrations at {}",
                path.display().to_string().as_str().purple(),
            );
            return Ok(Some(path));
        }

        let path = find_migrations(path, depth + 1, max_depth)?;

        if let Some(path) = path {
            return Ok(Some(path));
        }
    }

    Ok(None)
}

#[macro_export]
macro_rules! trace {
    ($($t:tt)*) => {{
        use colored::Colorize;
        if $crate::VERBOSE.load(std::sync::atomic::Ordering::Relaxed) {
            print!("{:5} | ", "TRACE".blue());
            println!($($t)*);
        }
    }};
}

#[macro_export]
macro_rules! info {
    ($($t:tt)*) => {{
        use colored::Colorize;
            print!("{:5} | ", "INFO".green());
            println!($($t)*);
    }};
}
