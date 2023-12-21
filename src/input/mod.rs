use structopt::StructOpt;

#[derive(StructOpt)]
pub struct Opt {
    /// Search term
    #[structopt()]
    pub search_term: String,

    /// Record limit
    #[structopt(short = "l", long, default_value = "25")]
    pub record_limit: usize,
}