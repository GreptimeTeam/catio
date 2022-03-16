use thiserror::Error;

pub type Result<T> = std::result::Result<T, CatioError>;

#[derive(Error, Debug)]
pub enum CatioError {
    #[error("syscall")]
    Syscall(#[from] nix::Error),
}
