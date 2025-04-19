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

    fn is_mirage(entry: &DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .map(|s| s.starts_with(".mirage"))
            .unwrap_or(false)
    }

    for here in walkdir::WalkDir::new(&target_dir)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|f| !is_mirage(f))
    {
        debug!("Try Processing file {:?}", here);
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
            .sort_by_file_name()
            .into_iter()
            .filter_entry(|f| !is_mirage(f))
        {
            debug!("Try Comparing file {:?}", here);
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
mod tests {
    use std::fs::{self, read_link, File};
    use std::io::Read;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use log::debug;
    use tempfile::tempdir;

    use crate::{apply, revert};

    enum TestFsObject {
        File {
            name: String,
            contents: String,
        },
        Dir {
            name: String,
            contents: Vec<TestFsObject>,
        },
    }

    struct TestFsView<'a> {
        base_obj: &'a TestFsObject,
        base_path: PathBuf,
    }

    impl<'a> TestFsView<'a> {
        fn new(base_obj: &'a TestFsObject, base_path: PathBuf) -> Self {
            TestFsView {
                base_obj,
                base_path,
            }
        }

        fn get_children(&self) -> Vec<TestFsView> {
            self.base_obj
                .get_children()
                .into_iter()
                .map(|f| TestFsView::new(f, self.base_path.join(self.base_obj.get_name())))
                .collect::<Vec<_>>()
        }

        fn is_symlink(&self) -> bool {
            self.base_obj.is_symlink(&self.base_path)
        }

        fn get_fs_metadata(&self) -> fs::Metadata {
            self.base_obj.get_path(&self.base_path).metadata().unwrap()
        }

        fn get_full_path(&self) -> PathBuf {
            self.base_obj.get_path(&self.base_path)
        }

        fn verify(&self) {
            // verify that the structure is correct that files are not symlinks
            // verify that the contents are correct
            // verify that directories are not symlinks

            // verify that the contents are correct
            if self.base_obj.is_symlink(&self.base_path) {
                panic!("Object is a symlink");
            }
            if self.get_fs_metadata().file_type().is_dir() {
                for child in self.get_children() {
                    child.verify();
                }
            } else {
                let mut file = File::open(self.get_full_path()).unwrap();
                let mut contents = String::new();
                file.read_to_string(&mut contents).unwrap();
                assert_eq!(contents, self.base_obj.get_contents());
            }
        }
    }

    impl TestFsObject {
        fn get_view(&self, base_path: &Path) -> TestFsView {
            TestFsView::new(self, base_path.to_path_buf())
        }

        fn create(&self, base_path: &Path) {
            match self {
                TestFsObject::File { name, contents } => {
                    let file_path = base_path.join(name);
                    let mut file = File::create(file_path).unwrap();
                    file.write_all(contents.as_bytes()).unwrap();
                    file.flush().unwrap();
                    file.sync_all().unwrap();
                }
                TestFsObject::Dir { name, contents } => {
                    let dir_path = base_path.join(name);
                    fs::create_dir(&dir_path).unwrap();
                    for content in contents {
                        content.create(&dir_path);
                    }
                }
            }
        }

        fn get_path(&self, base_path: &Path) -> PathBuf {
            match self {
                TestFsObject::File { name, .. } => base_path.join(name),
                TestFsObject::Dir { name, .. } => base_path.join(name),
            }
        }

        fn get_contents(&self) -> &str {
            match self {
                TestFsObject::File { contents, .. } => contents.as_str(),
                TestFsObject::Dir { .. } => panic!("Cannot get contents of a directory"),
            }
        }

        fn get_children(&self) -> &[TestFsObject] {
            match self {
                TestFsObject::Dir { contents, .. } => contents.as_slice(),
                TestFsObject::File { .. } => panic!("Cannot get children of a file"),
            }
        }

        fn is_symlink(&self, base_path: &Path) -> bool {
            match self {
                TestFsObject::File { name, .. } => {
                    debug!("Checking if file symlink for {:?} {:?}", base_path, name);
                    let file_path = base_path.join(name);
                    fs::symlink_metadata(file_path)
                        .map(|meta| meta.file_type().is_symlink())
                        .expect("Error checking symlink metadata")
                }
                TestFsObject::Dir { name, .. } => {
                    debug!("Checking if dir symlink for {:?} {:?}", base_path, name);
                    let dir_path = base_path.join(name);
                    fs::symlink_metadata(dir_path)
                        .map(|meta| meta.file_type().is_symlink())
                        .expect("Error checking symlink metadata")
                }
            }
        }

        fn get_name(&self) -> &str {
            match self {
                TestFsObject::File { name, .. } => name,
                TestFsObject::Dir { name, .. } => name,
            }
        }
    }

    #[test]
    fn simple_test() {
        pretty_env_logger::init();
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let test_dir = TestFsObject::Dir {
            name: "test_dir".to_string(),
            contents: vec![
                TestFsObject::File {
                    name: "file1.txt".to_string(),
                    contents: "duplicate content".to_string(),
                },
                TestFsObject::File {
                    name: "file2.txt".to_string(),
                    contents: "duplicate content".to_string(),
                },
                TestFsObject::File {
                    name: "file3.txt".to_string(),
                    contents: "unique content".to_string(),
                },
            ],
        };

        test_dir.create(dir_path);

        let test_view = test_dir.get_view(dir_path);

        let dir_path = test_dir.get_path(dir_path);

        apply(&dir_path).unwrap();

        let originals_dir = dir_path.join(".mirage/originals");

        // file1 should now be in .mirage/originals
        let orig1 = originals_dir.join("file1.txt");
        assert!(orig1.exists());

        // file1 should now be a symlink to file1 in .mirage/originals
        assert!(test_view.get_children()[0].is_symlink());

        assert!(test_view.get_children()[1].is_symlink());

        assert_eq!(
            fs::canonicalize(read_link(&test_view.get_children()[0].get_full_path()).unwrap())
                .unwrap(),
            fs::canonicalize(&orig1).unwrap()
        );

        assert_eq!(
            fs::canonicalize(read_link(&test_view.get_children()[1].get_full_path()).unwrap())
                .unwrap(),
            fs::canonicalize(&orig1).unwrap()
        );

        assert!(&test_view.get_children()[2]
            .get_fs_metadata()
            .file_type()
            .is_file());

        revert(&dir_path).unwrap();

        test_view.verify();

        // Check if mirage directory is removed

        assert!(!dir_path.join(".mirage").exists());
        assert!(!dir_path.join(".mirage/originals").exists());
        assert!(!dir_path.join(".mirage/wal.json").exists());
    }
}
