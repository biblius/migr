use postgres::Client;
use std::io::{self, stdin, Error, Write};

#[derive(Debug)]
pub struct Postgres {
    host: String,
    port: u16,
    user: String,
    database: String,
    password: String,
    state: ParserState,
}

impl Postgres {
    pub fn establish_connection(&self) -> Client {
        postgres::Client::connect(&self.to_url(), postgres::NoTls)
            .expect("Could not establish PG connection")
    }

    /// Tries to find the `DATABASE_URL` env variable. If unsuccessful prompts the user for config.
    pub fn parse() -> Result<Self, Error> {
        let db_url = std::env::var("DATABASE_URL");

        match db_url {
            Ok(url) => Ok(Self::from_url(url)),
            Err(_) => {
                let mut pg = Self::default();
                let mut buf = String::new();

                println!("No DATABASE_URL found, please enter the following:");
                loop {
                    pg.print();
                    io::stdout().flush()?;
                    stdin().read_line(&mut buf)?;
                    pg.set(buf.trim())?;
                    buf.clear();
                    if pg.state == ParserState::Done {
                        break;
                    }
                }
                std::env::set_var("DATABASE_URL", pg.to_url());
                Ok(pg)
            }
        }
    }

    pub fn to_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }

    fn next(&mut self) {
        self.state = (self.state as usize + 1).into()
    }

    fn set(&mut self, buf: &str) -> Result<(), Error> {
        match self.state {
            ParserState::Host => {
                if buf.is_empty() {
                    self.next();
                    return Ok(());
                }
                self.host = buf.to_string();
                self.next();
                Ok(())
            }
            ParserState::Port => {
                if buf.is_empty() {
                    self.next();
                    return Ok(());
                }
                self.port = buf
                    .parse()
                    .map_err(|e| Error::new(io::ErrorKind::InvalidData, e))?;
                self.next();
                Ok(())
            }
            ParserState::User => {
                self.user = buf.to_string();
                self.next();
                Ok(())
            }
            ParserState::DB => {
                self.database = buf.to_string();
                self.next();
                Ok(())
            }
            ParserState::PW => {
                self.password = buf.to_string();
                self.next();
                Ok(())
            }
            ParserState::Done => unreachable!(),
        }
    }

    fn print(&self) {
        match self.state {
            ParserState::Host => print!("Host ({}): ", self.host),
            ParserState::Port => print!("Port ({}): ", self.port),
            ParserState::DB => print!("Database ({}): ", self.database),
            ParserState::User => print!("User ({}): ", self.user),
            ParserState::PW => print!("Password ({}): ", self.password),
            ParserState::Done => println!("All done!"),
        }
    }

    fn from_url(url: String) -> Self {
        let mut this = Self::default();
        let url = url.replace("postgres://", "");
        url.split(&[':', '@', '/', '?'])
            .enumerate()
            .for_each(|(i, el)| match i {
                0 => this.user = el.to_string(),
                1 => this.password = el.to_string(),
                2 => this.host = el.to_string(),
                3 => this.port = el.parse().expect("Invalid port in URL"),
                4 => this.database = el.to_string(),
                _ => unimplemented!(),
            });

        this
    }
}

impl Default for Postgres {
    fn default() -> Self {
        Self {
            host: String::from("localhost"),
            port: 5432,
            user: String::from("postgres"),
            database: String::from("postgres"),
            password: String::from("postgres"),
            state: ParserState::Host,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParserState {
    Host,
    Port,
    User,
    DB,
    PW,
    Done,
}

impl From<usize> for ParserState {
    fn from(value: usize) -> Self {
        match value {
            0 => ParserState::Host,
            1 => ParserState::Port,
            2 => ParserState::User,
            3 => ParserState::DB,
            4 => ParserState::PW,
            5 => ParserState::Done,
            _ => unreachable!(),
        }
    }
}

impl Default for ParserState {
    fn default() -> Self {
        Self::Host
    }
}
