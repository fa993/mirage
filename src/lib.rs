use std::{
    collections::HashMap,
    fs::{self, create_dir, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read},
    path::{Path, PathBuf},
};

use log::{debug, trace, warn};
use serde::{Deserialize, Serialize};
use symlink::symlink_file;
use thiserror::Error;
use walkdir::DirEntry;

#[derive(Debug, Serialize, Deserialize)]
enum ActionType {
    Copy,
    Symlink,
    NOP,
}

#[derive(Debug, Serialize, Deserialize)]
struct Action {
    action: ActionType,
    source: PathBuf,
    target: PathBuf,
}

impl Action {
    pub fn new(action: ActionType, source: PathBuf, target: PathBuf) -> Self {
        Action {
            action,
            source,
            target,
        }
    }

    pub fn invert(&self) -> Self {
        return match self.action {
            ActionType::Copy => Action {
                action: ActionType::NOP,
                source: self.target.clone(),
                target: self.source.clone(),
            },
            ActionType::Symlink => Action {
                action: ActionType::Copy,
                source: self.target.clone(),
                target: self.source.clone(),
            },
            ActionType::NOP => Action {
                action: ActionType::NOP,
                source: self.source.clone(),
                target: self.target.clone(),
            },
        };
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct WAL {
    actions: Vec<Action>,
    redirections: HashMap<PathBuf, PathBuf>,
    checkpoint: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MirageState {
    source_path: PathBuf,
    wal: WAL,
}

impl MirageState {
    pub fn get<T: AsRef<Path>>(target_dir: T) -> Result<MirageState, MirageError> {
        // convert path to absolute path
        let target_dir = fs::canonicalize(target_dir.as_ref())?;
        debug!("Target dir is {:?}", target_dir);

        // create .mirage if does not exist
        let mirage_path = target_dir.join(".mirage");
        if mirage_path.exists() && !mirage_path.is_dir() {
            return Err(MirageError::DotMirageError);
        }
        if !mirage_path.exists() {
            create_dir(&mirage_path)?;
        }
        if !(mirage_path.exists() && mirage_path.is_dir()) {
            return Err(MirageError::DotMirageInInconsistentState);
        }

        // create director .mirage/originals if does not exist
        let originals_path = mirage_path.join("originals");
        if originals_path.exists() && !originals_path.is_dir() {
            return Err(MirageError::DotMirageError);
        }
        if !originals_path.exists() {
            create_dir(&originals_path)?;
        }

        // now create .mirage/wal.json

        let wal_path = mirage_path.join("wal.json");

        if wal_path.exists() && !wal_path.is_file() {
            return Err(MirageError::WALError);
        }

        debug!("Opening wal file {:?}", wal_path);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        debug!("Reading wal file {:?}", wal_path);

        if file.metadata()?.len() == 0 {
            debug!("File is empty, creating new wal");
            let wal = WAL::default();
            serde_json::to_writer_pretty(BufWriter::new(file), &wal)?;
            return Ok(MirageState {
                source_path: mirage_path,
                wal,
            });
        } else {
            debug!("File is not empty, reading wal");

            let wal = serde_json::from_reader(BufReader::new(file))?;

            Ok(MirageState {
                source_path: mirage_path,
                wal,
            })
        }
    }

    pub fn commit(&self) -> Result<(), MirageError> {
        let wal_path = self.source_path.join("wal.json");
        let file = OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(wal_path)?;
        serde_json::to_writer_pretty(BufWriter::new(file), &self.wal)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum MirageError {
    #[error("Couldn't create fs resource")]
    ErrorDuringIO(#[from] io::Error),
    #[error(".mirage exists and is not dir")]
    DotMirageError,
    #[error("inconsistent state error")]
    DotMirageInInconsistentState,
    #[error(".mirage/wal.json exists and is not file")]
    WALError,
    #[error("error in encoding/decoding json")]
    JsonError(#[from] serde_json::Error),
    #[error("error in listing files")]
    WalkDirError(#[from] walkdir::Error),
}

pub fn apply<T: AsRef<Path>>(target_dir: T) -> Result<(), MirageError> {
    let mut state = MirageState::get(&target_dir)?;

    fn is_hidden(entry: &DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .map(|s| s.starts_with("."))
            .unwrap_or(false)
    }

    for here in walkdir::WalkDir::new(&target_dir)
        .into_iter()
        .filter_entry(|f| !is_hidden(f))
    {
        // handle soft errors here
        if let Err(x) = here {
            warn!("Can't access {:?} due to {:?}", x.path(), x.io_error());
            continue;
        }
        let here = here.unwrap();
        if here.path_is_symlink() {
            trace!("Skipping symlink {:?}", here.path());
            continue;
        }
        if here.file_type().is_dir() {
            trace!("Skipping dir {:?}", here.path());
            continue;
        }
        let here = fs::canonicalize(here.path())?;
        debug!("Processing file {}", here.display());
        // compare with hash of other entries
        for there in walkdir::WalkDir::new(&target_dir)
            .into_iter()
            .filter_entry(|f| !is_hidden(f))
        {
            if let Err(x) = there {
                warn!("Can't access {:?} due to {:?}", x.path(), x.io_error());
                continue;
            }
            let there = there.unwrap();
            if there.path_is_symlink() {
                trace!("Skipping symlink {:?}", there.path());
                continue;
            }

            if there.file_type().is_dir() {
                trace!("Skipping dir {:?}", there.path());
                continue;
            }
            let there: PathBuf = fs::canonicalize(there.path())?;
            if here.as_path() == there.as_path() {
                continue;
            }
            debug!("Comparing file {} with {}", here.display(), there.display());
            let is_same = check_if_files_are_same(here.as_path(), there.as_path())?;
            if is_same {
                trace!("Files are same {:?} {:?}", here.as_path(), there.as_path());

                // first check if redirection exists

                let contains_1 = state.wal.redirections.contains_key(here.as_path());
                let contains_2 = state.wal.redirections.contains_key(there.as_path());

                if contains_1 && contains_2 {
                    debug!("Redirection exists, skipping {:?}", here.as_path());
                    continue;
                } else if contains_1 {
                    // just create a symlink to where here points to for there
                    let here_pt = state.wal.redirections.get(here.as_path()).unwrap();
                    let action = Action::new(
                        ActionType::Symlink,
                        there.as_path().to_path_buf(),
                        here_pt.clone(),
                    );
                    state.wal.actions.push(action);
                    state
                        .wal
                        .redirections
                        .insert(there.as_path().to_path_buf(), here_pt.clone());
                    state.commit()?;
                    debug!("Redirection exists, using it {:?}", here.as_path());
                    continue;
                } else if contains_2 {
                    // just create a symlink to where there points to for here
                    let there_pt = state.wal.redirections.get(there.as_path()).unwrap();
                    let action = Action::new(
                        ActionType::Symlink,
                        here.as_path().to_path_buf(),
                        there_pt.clone(),
                    );
                    state.wal.actions.push(action);
                    state
                        .wal
                        .redirections
                        .insert(here.as_path().to_path_buf(), there_pt.clone());
                    state.commit()?;
                    debug!("Redirection exists, using it {:?}", there.as_path());
                    continue;
                }

                // move first file into originals and point both files using symlinks
                // first write to WAL
                let original_path = state.source_path.join("originals");

                //TODO handle this unwrap nicely
                let original_path = original_path.join(here.as_path().file_name().unwrap());

                let action = Action::new(
                    ActionType::Copy,
                    here.as_path().to_path_buf(),
                    original_path.clone(),
                );

                state.wal.actions.push(action);

                let action = Action::new(
                    ActionType::Symlink,
                    here.as_path().to_path_buf(),
                    original_path.clone(),
                );

                state.wal.actions.push(action);

                let action = Action::new(
                    ActionType::Symlink,
                    there.as_path().to_path_buf(),
                    original_path.clone(),
                );

                state.wal.actions.push(action);

                state
                    .wal
                    .redirections
                    .insert(here.as_path().to_path_buf(), original_path.clone());

                state
                    .wal
                    .redirections
                    .insert(there.as_path().to_path_buf(), original_path.clone());

                state.commit()?;
            }
        }
    }

    for action in state.wal.actions.iter().skip(state.wal.checkpoint) {
        match action.action {
            ActionType::Copy => {
                debug!(
                    "Copying file from {:?} to {:?}",
                    action.source, action.target
                );
                fs::copy(action.source.as_path(), action.target.as_path())?;
            }
            ActionType::Symlink => {
                debug!(
                    "Creating symlink from {:?} to {:?}",
                    action.source, action.target
                );
                if action.source.exists() {
                    fs::remove_file(action.source.as_path())?;
                }
                // horrible convention should fix
                symlink_file(action.target.as_path(), action.source.as_path())?;
            }
            ActionType::NOP => {
                // do nothing
                debug!("NOP action, doing nothing");
            }
        }
        state.wal.checkpoint += 1;
        state.commit()?;
    }

    Ok(())
}

pub fn revert<T: AsRef<Path>>(target_dir: T) -> Result<(), MirageError> {
    let state = MirageState::get(&target_dir)?;

    for action in state
        .wal
        .actions
        .iter()
        .rev()
        .skip(state.wal.actions.len() - state.wal.checkpoint)
        .map(|f| f.invert())
    {
        match action.action {
            ActionType::Copy => {
                debug!(
                    "Copying file from {:?} to {:?}",
                    action.source, action.target
                );
                // TODO: this shouldn't be dangerous as target will always be symlinks
                if action.target.exists() {
                    fs::remove_file(action.target.as_path())?;
                }
                fs::copy(action.source.as_path(), action.target.as_path())?;
            }
            ActionType::Symlink => {
                symlink_file(action.source.as_path(), action.target.as_path())?;
            }
            ActionType::NOP => {
                // do nothing
                debug!("NOP action, doing nothing");
            }
        }
    }

    // remove .mirage directory

    let mirage_path = state.source_path;
    if mirage_path.exists() {
        fs::remove_dir_all(mirage_path)?;
    }

    Ok(())
}

pub fn check_if_files_are_same(here: &Path, there: &Path) -> Result<bool, MirageError> {
    // compare hashes of files
    let h_meta = here.metadata()?;
    let t_meta = there.metadata()?;
    if h_meta.len() != t_meta.len() {
        return Ok(false);
    }
    return full_match(here, there);
    // Ok(here_hash == there_hash)
}

pub fn full_match(here: &Path, there: &Path) -> Result<bool, MirageError> {
    let file1 = File::open(here)?;
    let mut reader1 = BufReader::new(file1);
    let file2 = File::open(there)?;
    let mut reader2 = BufReader::new(file2);
    let mut buf1 = [0; 10000];
    let mut buf2 = [0; 10000];
    loop {
        if let Result::Ok(n1) = reader1.read(&mut buf1) {
            if n1 > 0 {
                if let Result::Ok(n2) = reader2.read(&mut buf2) {
                    if n1 == n2 {
                        if buf1 == buf2 {
                            continue;
                        }
                    }
                    trace!("not equal");
                    return Ok(false);
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
    trace!("equal");
    return Ok(true);
}

#[cfg(test)]
mod tests {}
