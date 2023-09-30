# PG Migrator (migr)

A minimal CLI tool to manipulate PostgreSQL migrations.

```bash
cargo install migr
```

Run `migr` to see a list of available commands.
The tool requires the `DATABASE_URL` variable to be set in the process env.

## setup

To get started with a fresh migrationss directory run

```bash
migr setup
```

## gen

For generating migrations, it is advised you use

```bash
migr gen <NAME>
```

This will maintain correct ordering of migrations via timestamps.
If you ever choose to edit or create a migration manually and the ordering matters, ensure you change the timestamp accordingly.

## sync

```bash
migr sync [-t]
```

`-t` will remove migrations from the metadata table that don't exist in the directory.

## run/rev/redo

```bash
migr run/rev/redo [-c] [-a] [-e <NAME>]
```

`-c` is a count of how many migrations the action will be performed on.

`-a` will perform the action on all migrations.

`-e` performs the action on the exact migration. The name should be the exact migration name without the timestamp, e.g. ~~`XXXX-XX-XX-XXXXXX\_`~~ `create_table_foo`.
