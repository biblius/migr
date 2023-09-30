use crate::{info, trace, GenMigration, RunRevMigration};
use anyhow::{Context, Error};
use colored::Colorize;
use postgres::{Client, Transaction};
use std::collections::HashSet;
use std::fmt::{Display, Write};
use std::{fs, path::PathBuf};

const INITIAL: &str = "0000000000_pg_migrator";

const INITIAL_TABLE_QUERY: &str = "
CREATE TABLE __migr_meta__(
    id VARCHAR(255) PRIMARY KEY,
    pending BOOLEAN DEFAULT TRUE
)";

const INITIAL_ENTRY_QUERY: &str = "
INSERT INTO __migr_meta__ VALUES (0, TRUE)
";

pub fn migration_generate(
    args: &GenMigration,
    mut path: PathBuf,
    mut pg: Client,
) -> anyhow::Result<()> {
    check_table(&mut pg)?;
    let name = &args.name;
    let date = time::OffsetDateTime::now_utc();
    let (date, (h, m, s)) = (date.date(), date.time().as_hms());

    let full_name = format!("{date}-{h:02}{m:02}{s:02}_{name}");

    path.push(&full_name);

    info!(
        "Creating migration at {}",
        path.display().to_string().as_str().yellow()
    );

    fs::create_dir(&path)?;

    path.push("up.sql");

    info!(
        "Creating up migration at {}",
        path.display().to_string().as_str().green()
    );

    fs::write(&path, "")?;

    path.pop();
    path.push("down.sql");

    info!(
        "Creating down migration at {}",
        path.display().to_string().as_str().bright_red()
    );

    fs::write(path, "-- Revert everything from up.sql")?;

    trace!("Updating metadata table");

    pg.execute("INSERT INTO __migr_meta__ VALUES ($1, TRUE)", &[&full_name])
        .context("Could not insert into __migr_meta__")?;

    info!("Successfully generated migration {}", name.green());

    Ok(())
}

pub fn migration_run(args: &RunRevMigration, path: PathBuf, mut pg: Client) -> anyhow::Result<()> {
    check_table(&mut pg)?;

    if let Some(ref name) = args.exact {
        return find_and_execute(&path, name, &mut pg, UpDown::Up);
    }

    info!("Running migrations");
    let count = args.count;
    let count = migration_up(count, path, &mut pg)?;
    if count > 0 {
        info!("{count} migrations successfully executed");
    } else {
        info!("Migrations already up to date");
    }
    Ok(())
}

pub fn migration_rev(args: &RunRevMigration, path: PathBuf, mut pg: Client) -> anyhow::Result<()> {
    check_table(&mut pg)?;

    if let Some(ref name) = args.exact {
        return find_and_execute(&path, name, &mut pg, UpDown::Down);
    }

    info!("Reverting migrations");
    let count = args.count.or((!args.all).then_some(1));
    let count = migration_down(count, &path, &mut pg)?;
    if count > 0 {
        info!("{count} migrations successfully reverted");
    } else {
        info!("Migrations already up to date");
    }
    Ok(())
}

pub fn migration_redo(args: &RunRevMigration, path: PathBuf, mut pg: Client) -> anyhow::Result<()> {
    check_table(&mut pg)?;

    if let Some(ref name) = args.exact {
        find_and_execute(&path, name, &mut pg, UpDown::Down)?;
        return find_and_execute(&path, name, &mut pg, UpDown::Up);
    }

    info!("Redoing migrations");
    let count = args.count.or((!args.all).then_some(1));
    migration_down(count, &path, &mut pg)?;
    migration_up(count, path, &mut pg)?;
    info!("Successfully redone migrations");
    Ok(())
}

pub fn setup(mut path: PathBuf, pg: &mut Client) -> anyhow::Result<()> {
    info!("Creating metadata table");

    let query = format!("{INITIAL_TABLE_QUERY};{INITIAL_ENTRY_QUERY}");

    if let Err(err) = pg.batch_execute(&query) {
        let Some(e) = err.as_db_error() else {
            return Err(err.into());
        };

        if *e.code() != postgres::error::SqlState::DUPLICATE_TABLE {
            return Err(err.into());
        }

        return Err(err).context("The migr metadata table already exists. Run `migr sync` if you need to sync it with existing migrations.");
    };

    info!("Creating migrations directory");

    fs::create_dir(&path)
        .with_context(|| format!("Unable to create migrations at '{}'", path.display()))?;

    path.push(INITIAL);

    fs::create_dir(&path)
        .with_context(|| format!("Unable to create migration at '{}'", path.display()))?;

    path.push("up.sql");

    trace!("Setting up initial 'up' migration");

    fs::write(&path, "-- Set up initial SQL dependencies here")?;

    path.pop();
    path.push("down.sql");

    trace!("Setting up initial 'down' migration");

    fs::write(&path, "-- Revert everything from up.sql")?;

    info!(
        "Successfully set up migrations directory at {}",
        path.display().to_string().as_str().purple()
    );

    Ok(())
}

pub fn sync(trim: bool, path: &PathBuf, pg: &mut Client) -> anyhow::Result<()> {
    info!("Syncing existing migrations with migr");

    let mut mig_metas = match pg.query("SELECT id FROM __migr_meta__", &[]) {
        Ok(rows) => rows
            .into_iter()
            .map(|r| r.get::<usize, String>(0))
            .collect::<HashSet<_>>(),
        Err(err) => {
            let Some(e) = err.as_db_error() else {
                return Err(Error::new(err));
            };

            if *e.code() != postgres::error::SqlState::UNDEFINED_TABLE {
                return Err(Error::new(err));
            }

            pg.batch_execute(INITIAL_TABLE_QUERY)?;

            info!("Successfully created metadata table");

            HashSet::new()
        }
    };

    let mut mig_dirs = fs::read_dir(path)?
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .collect::<Vec<_>>();

    mig_dirs.sort_by_key(|e| e.file_name());

    let num_migs = mig_dirs.len();
    let query = mig_dirs
        .into_iter()
        .filter_map(|d| d.file_name().to_str().map(String::from))
        .enumerate()
        .fold(
            String::from("INSERT INTO __migr_meta__ VALUES "),
            |mut query, (i, mig_name)| {
                trace!("Syncing {} with metadata table", mig_name.blue());

                if i == num_migs - 1 {
                    // Ensures we only update entries not already present
                    write!(query, "('{mig_name}', TRUE) ON CONFLICT DO NOTHING").unwrap();
                } else {
                    write!(query, "('{mig_name}', TRUE),").unwrap();
                }

                mig_metas.remove(&mig_name);
                query
            },
        );

    pg.execute(&query, &[])
        .context("Could not insert into metadata table")?;

    if trim {
        for mig in mig_metas {
            info!("Trimming {}", mig.blue());
            pg.execute("DELETE FROM __migr_meta__ WHERE id = $1", &[&mig])?;
        }
    }

    info!("Successfully synced migr with existing migrations");

    Ok(())
}

fn migration_up(count: Option<usize>, path: PathBuf, pg: &mut Client) -> anyhow::Result<usize> {
    let paths = migration_files(&path, UpDown::Up)?;
    let meta = migration_meta(&paths, pg, UpDown::Up)?;
    migrations_execute(count, &paths, &meta, pg, UpDown::Up)
}

fn migration_down(count: Option<usize>, path: &PathBuf, pg: &mut Client) -> anyhow::Result<usize> {
    let mut paths = migration_files(path, UpDown::Down)?;
    paths.reverse();
    let meta = migration_meta(&paths, pg, UpDown::Down)?;
    migrations_execute(count, &paths, &meta, pg, UpDown::Down)
}

fn check_table(pg: &mut Client) -> anyhow::Result<()> {
    if let Err(err) = pg.query("SELECT id FROM __migr_meta__ WHERE id='0'", &[]) {
        let Some(e) = err.as_db_error() else {
            return Err(Error::new(err));
        };

        if *e.code() != postgres::error::SqlState::UNDEFINED_TABLE {
            return Err(Error::new(err));
        }

        return Err(err).context(
            "The metadata table does not exist.\nHint: Run `migr sync` to create it with existing migrations.",
        );
    }
    Ok(())
}

fn find_and_execute(path: &PathBuf, name: &str, pg: &mut Client, ud: UpDown) -> anyhow::Result<()> {
    let (path, id) = find_exact(path, name, pg)?;
    match ud {
        UpDown::Up => info!("Running migration {}", id.blue()),
        UpDown::Down => info!("Reverting migration {}", id.blue()),
    }
    let file = format!("{}/{ud}", path.display());
    let mut tx = pg.transaction()?;
    match migration_execute_exact(&file.into(), &id, &mut tx, ud) {
        Ok(_) => {
            tx.commit()?;
            Ok(())
        }
        Err(e) => {
            tx.rollback()?;
            Err(e)
        }
    }
}

/// Finds the exact migration by stripping the ts prefix in the name and returns its path and meta ID.
/// `path` is a path pointing to the migrations dir.
/// `name` is the name of the migration without the timestamp
fn find_exact(path: &PathBuf, name: &str, pg: &mut Client) -> anyhow::Result<(PathBuf, String)> {
    let Some(migration_path) = fs::read_dir(path)?
        .filter_map(Result::ok)
        .find(|f| {
            let path = f.path();
            let Some(full_name) = path.file_name() else {
                return false;
            };
            let Some(migration) = full_name.to_str().map(|n| n.to_string()) else {
                return false;
            };
            let Some(prefix_end) = migration.chars().position(|c| c == '_') else {
                return false;
            };
            name == &migration[prefix_end + 1..]
        })
        .map(|e| e.path())
    else {
        return Err(Error::msg(format!("No migration found for name '{name}'")));
    };

    let Some(name) = migration_path.file_name() else {
        return Err(Error::msg("Unsupported file found for migration"));
    };

    let Some(name) = name.to_str() else {
        return Err(Error::msg("Unsupported file found for migration"));
    };

    trace!(
        "Found migration {}",
        migration_path.display().to_string().blue()
    );

    let count = pg
        .query_one("SELECT COUNT(*) from __migr_meta__ WHERE id = $1", &[&name])?
        .get::<usize, i64>(0);

    if count == 0 {
        return Err(Error::msg(format!(
            "No entry found in metadata for {}\nHint: Run `migr sync` to sync the metadata table",
            name.red()
        )));
    }

    let name = name.to_string();

    Ok((migration_path, name))
}

fn migrations_execute(
    exec_count: Option<usize>,
    paths: &[PathBuf],
    meta: &[(String, bool)],
    pg: &mut Client,
    ud: UpDown,
) -> anyhow::Result<usize> {
    let mut count = 0;

    let mut tx = pg.build_transaction().start()?;

    for (path, (id, pending)) in paths.iter().zip(meta.iter()) {
        if let Some(exec_count) = exec_count {
            if count >= exec_count {
                break;
            }
        }

        if matches!(ud, UpDown::Up) && !pending {
            continue;
        }

        if matches!(ud, UpDown::Down) && *pending {
            continue;
        }

        if let Err(e) = migration_execute_exact(path, id, &mut tx, ud) {
            tx.rollback()?;
            return Err(e);
        };

        count += 1;

        info!("Executed {}", path.display().to_string().blue());
    }

    tx.commit()?;

    Ok(count)
}

fn migration_execute_exact(
    path: &PathBuf,
    id: &str,
    tx_outer: &mut Transaction<'_>,
    ud: UpDown,
) -> anyhow::Result<()> {
    let sql = fs::read_to_string(path)?;

    let mut tx = tx_outer.transaction()?;

    if let Err(e) = tx.batch_execute(&sql) {
        tx.rollback()?;
        return Err(e).with_context(|| {
            format!(
                "while executing migration {}",
                path.display().to_string().red(),
            )
        });
    }

    let query = match ud {
        UpDown::Up => "UPDATE __migr_meta__ SET pending=FALSE WHERE id=$1",
        UpDown::Down => "UPDATE __migr_meta__ SET pending=TRUE WHERE id=$1",
    };

    if let Err(e) = tx.execute(query, &[&id]) {
        tx.rollback()?;
        return Err(e).with_context(|| {
            format!(
                "while executing migration {}",
                path.display().to_string().red(),
            )
        });
    }

    tx.commit()?;

    match ud {
        UpDown::Up => info!("Successfully executed migration"),
        UpDown::Down => info!("Successfully reverted migration"),
    }

    Ok(())
}

fn migration_meta(
    paths: &[PathBuf],
    pg: &mut Client,
    ud: UpDown,
) -> Result<Vec<(String, bool)>, Error> {
    let mig_ids = paths
        .iter()
        .filter_map(|f| {
            let name = f.parent()?.file_name()?;
            name.to_str()
        })
        .collect::<Vec<_>>();

    let query = match ud {
        UpDown::Up => "SELECT * FROM __migr_meta__ WHERE id = ANY($1) ORDER BY id ASC",
        UpDown::Down => "SELECT * FROM __migr_meta__ WHERE id = ANY($1) ORDER BY id DESC",
    };

    let migs = match pg.query(query, &[&mig_ids]) {
        Ok(rows) => rows
            .into_iter()
            .map(|r| (r.get::<usize, String>(0), r.get::<usize, bool>(1))),
        Err(e) => return Err(Error::new(e)),
    };

    Ok(migs.collect())
}

fn migration_files(path: &PathBuf, ud: UpDown) -> Result<Vec<PathBuf>, Error> {
    let mig_dirs = fs::read_dir(path)?;
    let mut pending = vec![];
    let ty = match ud {
        UpDown::Up => "up.sql",
        UpDown::Down => "down.sql",
    };

    for mig in mig_dirs {
        let entry = mig?.path();

        if !entry.is_dir() {
            continue;
        }

        let updown = entry.read_dir()?;

        let file = updown
            .filter_map(Result::ok)
            .find(|e| match e.file_name().into_string() {
                Ok(e) => e.contains(ty),
                Err(_) => false,
            })
            .ok_or_else(|| {
                Error::msg(format!(
                    "{} does not contain the necessary `{ty}` file.",
                    entry.display(),
                ))
            })?;

        pending.push(file.path())
    }

    pending.sort();

    Ok(pending)
}

#[derive(Debug, Clone, Copy)]
enum UpDown {
    Up,
    Down,
}

impl Display for UpDown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpDown::Up => write!(f, "up.sql"),
            UpDown::Down => write!(f, "down.sql"),
        }
    }
}
