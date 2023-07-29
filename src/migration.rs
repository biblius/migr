use crate::{GenMigration, RedoMigration, RunRevMigration};
use postgres::Client;
use std::{
    fs,
    io::{self, Error},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const INITIAL: &str = "0000000000_pg_migrator";

pub fn migration_generate(args: GenMigration, path: &str, mut pg: Client) -> Result<(), Error> {
    let init_exists = fs::read_dir(path)?.any(|dir| {
        dir.as_ref()
            .is_ok_and(|entry| entry.file_name().to_str().is_some_and(|e| e == INITIAL))
    });

    if !init_exists {
        if !args.force {
            return Err(Error::new(
                io::ErrorKind::Other,
                "Initial migration could not be found, run with -f to create it",
            ));
        }
        println!("Creating initial migration file");
        setup(path, &mut pg)?;
    }

    let name = &args.name;
    let time = timestamp();

    let mig_path = format!("{path}/{time}_{name}");

    println!("Setting up metadata table");

    pg.execute("INSERT INTO __pgm_meta__ VALUES ($1, TRUE)", &[&time])
        .expect("Could not insert into ");

    fs::create_dir(&mig_path)?;
    fs::write(mig_path.clone() + "/up.sql", "")?;
    fs::write(mig_path + "/down.sql", "--Revert everything from up.sql")?;
    println!("Successfully generated migration {name}");
    Ok(())
}

pub fn migration_run(args: RunRevMigration, path: &str, mut pg: Client) -> Result<(), Error> {
    println!("Running migrations");
    migration_up(args, path, &mut pg)
}

pub fn migration_rev(mut args: RunRevMigration, path: &str, mut pg: Client) -> Result<(), Error> {
    println!("Reverting migrations");
    if args.count.is_none() {
        args.count = Some(1)
    }
    migration_down(args, path, &mut pg)
}

pub fn migration_redo(redo: RedoMigration, path: &str, mut pg: Client) -> Result<(), Error> {
    println!("Redoing migrations");
    let args = if redo.all {
        RunRevMigration { count: None }
    } else {
        RunRevMigration { count: Some(1) }
    };
    migration_down(args, path, &mut pg)?;
    migration_up(args, path, &mut pg)
}

fn migration_up(args: RunRevMigration, path: &str, pg: &mut Client) -> Result<(), Error> {
    let paths = migration_files(path, UpDown::Up)?;
    let meta = migration_meta(&paths, pg, UpDown::Up)?;
    migration_execute(args, &paths, meta, pg, UpDown::Up)
}

fn migration_down(args: RunRevMigration, path: &str, pg: &mut Client) -> Result<(), Error> {
    let mut paths = migration_files(path, UpDown::Down)?;
    paths.reverse();
    let meta = migration_meta(&paths, pg, UpDown::Down)?;
    migration_execute(args, &paths, meta, pg, UpDown::Down)
}

pub fn setup(path: &str, pg: &mut Client) -> Result<(), Error> {
    let path = format!("{path}/{INITIAL}");

    pg.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS __pgm_meta__(id BIGINT PRIMARY KEY, pending BOOLEAN DEFAULT TRUE);
        INSERT INTO __pgm_meta__ VALUES (0, TRUE) ON CONFLICT (id) DO UPDATE SET pending=TRUE
        ",
    )
    .expect("Could not create initial table");

    if let Err(e) = fs::create_dir(&path) {
        if !matches!(e.kind(), io::ErrorKind::AlreadyExists) {
            return Err(e);
        }
    }

    fs::write(
        path.clone() + "/up.sql",
        "\
-- Sets up a trigger for the given table to automatically set a column called
-- `updated_at` whenever the row is modified (unless `updated_at` was included
-- in the modified columns)
--
-- # Example
--
-- ```sql
-- CREATE TABLE users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT NOW());
--
-- SELECT pgm_manage_updated_at('users');
-- ```
;

CREATE OR REPLACE FUNCTION pgm_manage_updated_at(_tbl regclass) RETURNS VOID AS $$

BEGIN
    EXECUTE format('CREATE TRIGGER set_updated_at BEFORE UPDATE ON %s
    FOR EACH ROW EXECUTE PROCEDURE pgm_set_updated_at()', _tbl);
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgm_set_updated_at() RETURNS trigger AS $$
BEGIN
    IF (
        NEW IS DISTINCT FROM OLD AND
        NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at
    ) THEN
        NEW.updated_at := current_timestamp;
    END IF;
    RETURN NEW;
END;

$$ LANGUAGE plpgsql;
",
    )?;

    fs::write(
        path + "/down.sql",
        "\
-- This file was automatically created by the migrator to setup helper functions
-- and other internal bookkeeping. This file is safe to edit, any future
-- changes will be added to existing projects as new migrations.
DROP FUNCTION IF EXISTS pgm_manage_updated_at(_tbl regclass);

DROP FUNCTION IF EXISTS pgm_set_updated_at();
",
    )?;

    Ok(())
}

fn migration_execute(
    args: RunRevMigration,
    paths: &[PathBuf],
    meta: impl Iterator<Item = (i64, bool)>,
    pg: &mut Client,
    ud: UpDown,
) -> Result<(), Error> {
    let mut count = 0;

    for (path, (id, pending)) in paths.iter().zip(meta) {
        if let Some(c) = args.count {
            if count >= c {
                break;
            }
        }

        match ud {
            UpDown::Up => {
                if !pending {
                    continue;
                }
            }
            UpDown::Down => {
                if pending {
                    continue;
                }
            }
        }

        let sql = fs::read_to_string(path)?;

        let name = path.to_str().unwrap_or_default();
        let name = name.rsplit('/').nth(1).unwrap();

        let mut tx = pg
            .build_transaction()
            .start()
            .expect("Error when starting transaction");

        if let Err(e) = tx.batch_execute(&sql) {
            tx.rollback().expect("Error when rolling back transaction");
            panic!("Error when running migration {e}");
        }

        let query = match ud {
            UpDown::Up => "UPDATE __pgm_meta__ SET pending=FALSE WHERE id=$1",
            UpDown::Down => "UPDATE __pgm_meta__ SET pending=TRUE WHERE id=$1",
        };
        if let Err(e) = tx.execute(query, &[&id]) {
            tx.rollback().expect("Error when rolling back transaction");
            panic!("Error in attempt to update migrations {e}");
        }

        tx.commit().expect("Error when committing transaction");

        count += 1;

        match ud {
            UpDown::Up => println!("Executed up migration: {name}"),
            UpDown::Down => println!("Executed down migration: {name}"),
        }
    }

    match ud {
        UpDown::Up => println!("Migrations successfully executed"),
        UpDown::Down => println!("Migrations successfully reverted"),
    }

    Ok(())
}

fn migration_meta(
    paths: &[PathBuf],
    pg: &mut Client,
    ud: UpDown,
) -> Result<impl Iterator<Item = (i64, bool)>, Error> {
    let mig_ids = paths
        .iter()
        .map(|f| {
            let f = f
                .to_str()
                .expect("Funky file detected")
                .rsplit('/')
                .nth(1)
                .unwrap();

            let ts = f
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>();

            Some(ts.parse::<i64>().unwrap())
        })
        .collect::<Vec<_>>();

    let query = match ud {
        UpDown::Up => "SELECT * FROM __pgm_meta__ WHERE id = ANY($1) ORDER BY id ASC",
        UpDown::Down => "SELECT * FROM __pgm_meta__ WHERE id = ANY($1) ORDER BY id DESC",
    };

    let migs = match pg.query(query, &[&mig_ids]) {
        Ok(rows) => rows
            .into_iter()
            .map(|r| (r.get::<usize, i64>(0), r.get::<usize, bool>(1))),
        Err(e) => {
            let err = e.to_string();
            if err.contains("relation \"__pgm_meta__\" does not exist") {
                return Err(Error::new(
                    io::ErrorKind::Other,
                    "The metadata table does not exist, have you run `pgm setup`?",
                ));
            } else {
                return Err(Error::new(io::ErrorKind::Other, err));
            }
        }
    };

    Ok(migs)
}

fn migration_files(path: &str, ud: UpDown) -> Result<Vec<PathBuf>, Error> {
    let rd = fs::read_dir(path)?;
    let mut pending = vec![];
    let ty = match ud {
        UpDown::Up => "up.sql",
        UpDown::Down => "down.sql",
    };

    for mig in rd {
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
            .unwrap_or_else(|| {
                panic!(
                    "{} does not contain the necessary `{ty}` file.",
                    entry.display(),
                )
            });
        pending.push(file.path())
    }
    pending.sort();
    Ok(pending)
}

fn timestamp() -> i64 {
    // Number of seconds from 1970-01-01 to 2000-01-01
    const TIME_SEC_CONVERSION: u64 = 946684800;
    const NSEC_PER_USEC: u64 = 1000;
    const USEC_PER_SEC: u64 = 1000000;

    let epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

    let to_usec =
        |d: Duration| d.as_secs() * USEC_PER_SEC + u64::from(d.subsec_nanos()) / NSEC_PER_USEC;

    match SystemTime::now().duration_since(epoch) {
        Ok(duration) => to_usec(duration) as i64,
        Err(e) => -(to_usec(e.duration()) as i64),
    }
}

enum UpDown {
    Up,
    Down,
}
