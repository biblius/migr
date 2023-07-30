# PG Migrator (pgm)

A simple CLI tool to manipulate SQL migrations. If you're like me, you absolutely dread ORM based migrations. This tool allows us to write migrations *a la diesel*, i.e. in pure SQL like god intended.

To get started, run

```bash
pgm setup
```

This will create a `migrations` directory if one is not found (all commands search for it 2 levels deep from the current dir) and will create the initial migration, as well as the metadata table in whichever DB was set.
The tool will search for `DATABASE_URL` in the current process env - if it can't be found you will be prompted to enter the credentials. You can skip this behaviour by exporting the `DATABASE_URL` beforehand or passing it in before the actual command.

Run `pgm` to see a list of available commands.
