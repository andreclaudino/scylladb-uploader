#[derive(clap::ValueEnum, Debug, Clone)]
pub enum FileType {
    JSON,
    CSV
}