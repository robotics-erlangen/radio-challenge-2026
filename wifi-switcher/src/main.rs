#[cfg(unix)]
mod unix;

fn main() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        unix::run()
    }
    #[cfg(not(unix))]
    {
        eprintln!("wifi-switcher is only supported on unix-like platforms");
        std::process::exit(1);
    }
}
