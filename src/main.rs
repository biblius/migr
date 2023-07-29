use crate::migration::migration_generate;
use clap::{Args, Parser, Subcommand};
use migration::{migration_redo, migration_rev, migration_run, setup};
use pg::Postgres;
use std::{env, process::Command};

mod migration;
mod pg;

fn main() -> Result<(), std::io::Error> {
    let pgm = Pgm::parse();

    let config = Postgres::parse()?;
    let path = if let Some(p) = pgm.path {
        p
    } else if let MigrationSubcommand::Setup = pgm.command {
        match get_absolute_migration_path() {
            Ok(p) => p,
            Err(_) => {
                println!("Migration directory not found, creating.");
                let pwd = env::current_dir()?;
                let pwd = pwd.to_str().unwrap();
                let path = format!("{pwd}/migrations");
                std::fs::create_dir(&path)?;
                path
            }
        }
    } else {
        get_absolute_migration_path().unwrap()
    };

    let mut pg = config.establish_connection();

    if let Err(e) = {
        match pgm.command {
            MigrationSubcommand::Setup => setup(&path, &mut pg),
            MigrationSubcommand::Gen(args) => migration_generate(args, &path, pg),
            MigrationSubcommand::Run(args) => migration_run(args, &path, pg),
            MigrationSubcommand::Rev(args) => migration_rev(args, &path, pg),
            MigrationSubcommand::Redo(args) => migration_redo(args, &path, pg),
        }
    } {
        println!("{e}");
    }
    Ok(())
}

#[derive(Debug, Parser)]
#[command(name = "pgm", author = "biblius", version = "0.1", about = "Minimal PG migration tool", long_about = None)]
pub struct Pgm {
    #[clap(subcommand)]
    pub command: MigrationSubcommand,

    /// If provided, pgm will search for migrations in the given directory.
    #[arg(long, short)]
    path: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum MigrationSubcommand {
    /// Initialise a migration directory in the current one and set up the initial migration and metadata table.
    Setup,
    /// Generate a new migration
    Gen(GenMigration),
    /// Run pending migrations
    Run(RunRevMigration),
    /// Reverse migrations
    Rev(RunRevMigration),
    /// Redo migrations
    Redo(RedoMigration),
}

#[derive(Debug, Args, Default, Clone)]
/// Migration arguments
pub struct RedoMigration {
    /// If given this will redo all migrations
    #[arg(long, short, action)]
    pub all: bool,
}

#[derive(Debug, Args, Default, Clone)]
/// Migration arguments
pub struct GenMigration {
    /// Migration name
    pub name: String,

    /// If a migration directory does not exist, this will call `setup` beforehand
    #[arg(long, short, action)]
    pub force: bool,
}

#[derive(Debug, Args, Default, Clone, Copy)]
/// Migration arguments
pub struct RunRevMigration {
    /// Migration name
    #[arg(long, short)]
    pub count: Option<usize>,
}

/// Gets the absolute path of the directory where migrations are located. Used to set process' working directory.
fn get_absolute_migration_path() -> Result<String, std::io::Error> {
    // Grab the current directory
    let pwd = env::current_dir()?;
    let pwd = pwd.to_str().unwrap();
    println!("Searching for migration directory in {pwd}");

    let mig_dir = Command::new("find")
        .args([".", "-name", "migrations", "-type", "d", "-maxdepth", "2"])
        .output()
        .unwrap()
        .stdout;

    if mig_dir.is_empty() {
        panic!("Migrations directory not found")
    }

    let path = String::from_utf8(mig_dir[1..].to_vec()).unwrap();
    let path = path.trim();
    println!("Found migrations directory at: {path}");

    Ok(format!("{pwd}{path}"))
}
