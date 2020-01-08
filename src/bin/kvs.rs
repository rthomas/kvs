use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KeyNotFoundError, KvStore, Result};

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("get")
                .about("Gets a value from the KV store.")
                .arg(
                    Arg::with_name("KEY")
                        .required(true)
                        .help("The key to fetch."),
                ),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Sets a value for a key in the KV store.")
                .arg(Arg::with_name("KEY").required(true).help("The key to set."))
                .arg(
                    Arg::with_name("VAL")
                        .required(true)
                        .help("The value to set."),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a value from the KV store.")
                .arg(
                    Arg::with_name("KEY")
                        .required(true)
                        .help("The key to remove."),
                ),
        )
        .subcommand(
            SubCommand::with_name("compact")
                .about("Compacts the KV Store file."),
        )
        .get_matches();

    let mut kv_store = KvStore::open(std::env::current_dir()?.as_path())?;

    if let Some(cmd) = matches.subcommand_matches("get") {
        let key = cmd.value_of("KEY").unwrap().to_string();
        match kv_store.get(key) {
            Ok(Some(val)) => {
                println!("{}", val);
            }
            Ok(None) => {
                println!("Key not found");
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        };
    }

    if let Some(cmd) = matches.subcommand_matches("set") {
        let key = cmd.value_of("KEY").unwrap().to_string();
        let val = cmd.value_of("VAL").unwrap().to_string();
        kv_store.set(key, val)?;
    }

    if let Some(cmd) = matches.subcommand_matches("rm") {
        let key = cmd.value_of("KEY").unwrap().to_string();
        match kv_store.remove(key) {
            Ok(_) => {}
            Err(e) => {
                match e.downcast::<KeyNotFoundError>() {
                    Ok(_) => {
                        println!("Key not found");
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                };
                std::process::exit(1);
            }
        }
    }

    if let Some(_) = matches.subcommand_matches("compact") {
        kv_store.compact_log()?;
    }

    Ok(())
}
