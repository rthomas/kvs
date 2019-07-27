use clap::{App, AppSettings, Arg, SubCommand};

fn main() {
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
        .get_matches();
    match matches.subcommand_name() {
        Some("get") => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Some("set") => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Some("rm") => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Some(_) | None => {}
    }
}
