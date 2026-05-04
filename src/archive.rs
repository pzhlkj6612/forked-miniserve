use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use libflate::gzip::Encoder;
use serde::Deserialize;
use strum::{Display, EnumIter, EnumString};
use tar::{Builder, EntryType, Header, HeaderMode};
use zip::{ZipWriter, write};

use crate::errors::RuntimeError;

/// Available archive methods
#[derive(Deserialize, Clone, Copy, EnumIter, EnumString, Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ArchiveMethod {
    /// Gzipped tarball
    TarGz,

    /// Regular tarball
    Tar,

    /// Regular zip
    Zip,
}

impl ArchiveMethod {
    pub fn extension(self) -> String {
        match self {
            Self::TarGz => "tar.gz",
            Self::Tar => "tar",
            Self::Zip => "zip",
        }
        .to_string()
    }

    pub fn content_type(self) -> String {
        match self {
            Self::TarGz => "application/gzip",
            Self::Tar => "application/tar",
            Self::Zip => "application/zip",
        }
        .to_string()
    }

    pub fn is_enabled(self, tar_enabled: bool, tar_gz_enabled: bool, zip_enabled: bool) -> bool {
        match self {
            Self::TarGz => tar_gz_enabled,
            Self::Tar => tar_enabled,
            Self::Zip => zip_enabled,
        }
    }

    /// Make an archive out of the given directory, and write the output to the given writer.
    ///
    /// Recursively includes all files and subdirectories.
    ///
    /// If `skip_symlinks` is `true`, symlinks fill not be followed and will just be ignored.
    pub fn create_archive<T, W>(
        self,
        dir: T,
        skip_symlinks: bool,
        out: W,
    ) -> Result<(), RuntimeError>
    where
        T: AsRef<Path>,
        W: std::io::Write,
    {
        let dir = dir.as_ref();
        match self {
            Self::TarGz => tar_gz(dir, skip_symlinks, out),
            Self::Tar => tar_dir(dir, skip_symlinks, out),
            Self::Zip => zip_dir(dir, skip_symlinks, out),
        }
    }
}

/// Write a gzipped tarball of `dir` in `out`.
fn tar_gz<W>(dir: &Path, skip_symlinks: bool, out: W) -> Result<(), RuntimeError>
where
    W: std::io::Write,
{
    let mut out = Encoder::new(out).map_err(|e| RuntimeError::IoError("GZIP".to_string(), e))?;

    tar_dir(dir, skip_symlinks, &mut out)?;

    out.finish()
        .into_result()
        .map_err(|e| RuntimeError::IoError("GZIP finish".to_string(), e))?;

    Ok(())
}

/// Write a tarball of `dir` in `out`.
///
/// The target directory will be saved as a top-level directory in the archive.
///
/// For example, consider this directory structure:
///
/// ```ignore
/// a
/// └── b
///     └── c
///         ├── e
///         ├── f
///         └── g
/// ```
///
/// Making a tarball out of `"a/b/c"` will result in this archive content:
///
/// ```ignore
/// c
/// ├── e
/// ├── f
/// └── g
/// ```
fn tar_dir<W>(dir: &Path, skip_symlinks: bool, out: W) -> Result<(), RuntimeError>
where
    W: std::io::Write,
{
    let inner_folder = dir.file_name().ok_or_else(|| {
        RuntimeError::InvalidPathError("Directory name terminates in \"..\"".to_string())
    })?;

    let directory = inner_folder.to_str().ok_or_else(|| {
        RuntimeError::InvalidPathError(
            "Directory name contains invalid UTF-8 characters".to_string(),
        )
    })?;

    tar(dir, directory.to_string(), skip_symlinks, out)
        .map_err(|e| RuntimeError::ArchiveCreationError("tarball".to_string(), Box::new(e)))
}

/// Writes a tarball of `dir` in `out`.
///
/// The content of `src_dir` will be saved in the archive as a folder named `inner_folder`.
fn tar<W>(
    src_dir: &Path,
    inner_folder: String,
    skip_symlinks: bool,
    out: W,
) -> Result<(), RuntimeError>
where
    W: std::io::Write,
{
    let mut tar_builder = Builder::new(out);

    append_tar_entries(
        &mut tar_builder,
        src_dir,
        &PathBuf::from(inner_folder),
        skip_symlinks,
    )?;

    // Finish the archive
    tar_builder.into_inner().map_err(|e| {
        RuntimeError::IoError("Failed to finish writing the TAR archive".to_string(), e)
    })?;

    Ok(())
}

fn append_tar_entries<W: Write>(
    tar_builder: &mut Builder<W>,
    src_dir: &Path,
    archive_root: &Path,
    skip_symlinks: bool,
) -> Result<(), RuntimeError> {
    let mut stack = vec![src_dir.to_path_buf()];

    while let Some(path) = stack.pop() {
        let metadata = fs::symlink_metadata(&path).map_err(|e| {
            RuntimeError::IoError(
                format!(
                    "Could not get file metadata of '{}'",
                    path.to_string_lossy()
                ),
                e,
            )
        })?;

        let relative_path = path
            .strip_prefix(src_dir)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::new());
        let destination = archive_root.join(relative_path);

        if metadata.file_type().is_symlink() {
            if skip_symlinks {
                continue;
            }

            let link_target = fs::read_link(&path).map_err(|e| {
                RuntimeError::IoError(
                    format!("Could not read symlink '{}'", path.to_string_lossy()),
                    e,
                )
            })?;
            let mut header = Header::new_gnu();
            header.set_metadata_in_mode(&metadata, HeaderMode::Complete);
            header.set_entry_type(EntryType::Symlink);
            header.set_size(0);
            tar_builder
                .append_link(&mut header, &destination, &link_target)
                .map_err(|e| {
                    RuntimeError::IoError(
                        format!(
                            "Failed to append symlink '{}' to the TAR archive",
                            destination.to_string_lossy()
                        ),
                        e,
                    )
                })?;
        } else if metadata.is_dir() {
            tar_builder.append_dir(&destination, &path).map_err(|e| {
                RuntimeError::IoError(
                    format!(
                        "Failed to append directory '{}' to the TAR archive",
                        destination.to_string_lossy()
                    ),
                    e,
                )
            })?;

            for entry in fs::read_dir(&path).map_err(|e| {
                RuntimeError::IoError(
                    format!("Could not read directory '{}'", path.to_string_lossy()),
                    e,
                )
            })? {
                let entry = entry.map_err(|e| {
                    RuntimeError::IoError("Could not read directory entry".to_string(), e)
                })?;
                stack.push(entry.path());
            }
        } else {
            tar_builder
                .append_path_with_name(&path, &destination)
                .map_err(|e| {
                    RuntimeError::IoError(
                        format!(
                            "Failed to append file '{}' to the TAR archive",
                            destination.to_string_lossy()
                        ),
                        e,
                    )
                })?;
        }
    }

    Ok(())
}

/// Write a zip of `dir` in `out`.
///
/// The target directory will be saved as a top-level directory in the archive.
///
/// For example, consider this directory structure:
///
/// ```ignore
/// a
/// └── b
///     └── c
///         ├── e
///         ├── f
///         └── g
/// ```
///
/// Making a zip out of `"a/b/c"` will result in this archive content:
///
/// ```ignore
/// c
/// ├── e
/// ├── f
/// └── g
/// ```
fn create_zip_from_directory<W>(
    out: W,
    directory: &Path,
    skip_symlinks: bool,
) -> Result<(), RuntimeError>
where
    W: std::io::Write + std::io::Seek,
{
    let options =
        write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let symlink_options: write::FileOptions<'static, ()> =
        write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let mut paths_queue: Vec<PathBuf> = vec![directory.to_path_buf()];
    let zip_root_folder_name = directory.file_name().ok_or_else(|| {
        RuntimeError::InvalidPathError("Directory name terminates in \"..\"".to_string())
    })?;

    let mut zip_writer = ZipWriter::new(out);
    let mut buffer = Vec::new();
    while !paths_queue.is_empty() {
        let next = paths_queue.pop().ok_or_else(|| {
            RuntimeError::ArchiveCreationDetailError("Could not get path from queue".to_string())
        })?;
        let current_dir = next.as_path();
        let directory_entry_iterator = std::fs::read_dir(current_dir)
            .map_err(|e| RuntimeError::IoError("Could not read directory".to_string(), e))?;
        let zip_directory = Path::new(zip_root_folder_name).join(
            current_dir.strip_prefix(directory).map_err(|_| {
                RuntimeError::ArchiveCreationDetailError(
                    "Could not append base directory".to_string(),
                )
            })?,
        );

        for entry in directory_entry_iterator {
            let entry_path = entry
                .ok()
                .ok_or_else(|| {
                    RuntimeError::InvalidPathError(
                        "Directory name terminates in \"..\"".to_string(),
                    )
                })?
                .path();
            let entry_metadata = std::fs::symlink_metadata(entry_path.clone()).map_err(|e| {
                RuntimeError::IoError(
                    format!(
                        "Could not get file metadata of '{}'",
                        entry_path.to_string_lossy()
                    )
                    .to_string(),
                    e,
                )
            })?;

            let current_entry_name = entry_path.file_name().ok_or_else(|| {
                RuntimeError::InvalidPathError("Invalid file or directory name".to_string())
            })?;

            // To let every software correctly parse the file structure in ZIP files that are produced
            // on any platform (esp. Windows), always use forward slashes. The documentation:
            // https://users.cs.jmu.edu/buchhofp/forensics/formats/pkzip.html
            let relative_path = if cfg!(windows) {
                let branch = zip_directory
                    .as_os_str()
                    .to_string_lossy()
                    .trim_end_matches(r"\") // every branch ends with two backslashes "\\".
                    .replace(r"\", "/"); // every branch uses backslash "\" as path separators.
                let leaf = current_entry_name.to_string_lossy();
                format!("{branch}/{leaf}") // construct a Unix-style path in the simplest way.
            } else {
                zip_directory
                    .join(current_entry_name)
                    .into_os_string()
                    .to_string_lossy()
                    .into_owned()
            };

            if entry_metadata.file_type().is_symlink() {
                if skip_symlinks {
                    continue;
                }

                let link_target = std::fs::read_link(&entry_path).map_err(|e| {
                    RuntimeError::IoError(
                        format!("Could not read symlink '{}'", entry_path.to_string_lossy()),
                        e,
                    )
                })?;

                zip_writer
                    .add_symlink_from_path(&relative_path, &link_target, symlink_options.clone())
                    .map_err(|_| {
                        RuntimeError::ArchiveCreationDetailError(
                            "Could not add symlink path to ZIP".to_string(),
                        )
                    })?;

                continue;
            }

            if entry_metadata.is_file() {
                let mut f = File::open(&entry_path)
                    .map_err(|e| RuntimeError::IoError("Could not open file".to_string(), e))?;
                f.read_to_end(&mut buffer).map_err(|e| {
                    RuntimeError::IoError("Could not read from file".to_string(), e)
                })?;
                zip_writer.start_file(relative_path, options).map_err(|_| {
                    RuntimeError::ArchiveCreationDetailError(
                        "Could not add file path to ZIP".to_string(),
                    )
                })?;
                zip_writer.write(buffer.as_ref()).map_err(|_| {
                    RuntimeError::ArchiveCreationDetailError(
                        "Could not write file to ZIP".to_string(),
                    )
                })?;
                buffer.clear();
            } else if entry_metadata.is_dir() {
                zip_writer
                    .add_directory(relative_path, options)
                    .map_err(|_| {
                        RuntimeError::ArchiveCreationDetailError(
                            "Could not add directory path to ZIP".to_string(),
                        )
                    })?;
                paths_queue.push(entry_path.clone());
            }
        }
    }

    zip_writer.finish().map_err(|_| {
        RuntimeError::ArchiveCreationDetailError("Could not finish writing ZIP archive".to_string())
    })?;
    Ok(())
}

/// Writes a zip of `dir` in `out`.
///
/// The content of `src_dir` will be saved in the archive as the  folder named .
fn zip_data<W>(src_dir: &Path, skip_symlinks: bool, mut out: W) -> Result<(), RuntimeError>
where
    W: std::io::Write,
{
    let mut data = Vec::new();
    let memory_file = Cursor::new(&mut data);
    create_zip_from_directory(memory_file, src_dir, skip_symlinks).map_err(|e| {
        RuntimeError::ArchiveCreationError(
            "Failed to create the ZIP archive".to_string(),
            Box::new(e),
        )
    })?;

    out.write_all(data.as_mut_slice())
        .map_err(|e| RuntimeError::IoError("Failed to write the ZIP archive".to_string(), e))?;

    Ok(())
}

fn zip_dir<W>(dir: &Path, skip_symlinks: bool, out: W) -> Result<(), RuntimeError>
where
    W: std::io::Write,
{
    let inner_folder = dir.file_name().ok_or_else(|| {
        RuntimeError::InvalidPathError("Directory name terminates in \"..\"".to_string())
    })?;

    inner_folder.to_str().ok_or_else(|| {
        RuntimeError::InvalidPathError(
            "Directory name contains invalid UTF-8 characters".to_string(),
        )
    })?;

    zip_data(dir, skip_symlinks, out)
        .map_err(|e| RuntimeError::ArchiveCreationError("zip".to_string(), Box::new(e)))
}
