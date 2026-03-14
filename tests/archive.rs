use std::io::{Cursor, Read};

use libflate::gzip::Decoder;
use reqwest::{StatusCode, blocking::Client};
use rstest::rstest;
use select::{document::Document, predicate::Text};
use tar::{Archive, EntryType};
use zip::ZipArchive;

mod fixtures;

use crate::fixtures::{Error, TestServer, reqwest_client, server};

enum ArchiveKind {
    TarGz,
    Tar,
    Zip,
}

impl ArchiveKind {
    fn server_option(&self) -> &'static str {
        match self {
            ArchiveKind::TarGz => "--enable-tar-gz",
            ArchiveKind::Tar => "--enable-tar",
            ArchiveKind::Zip => "--enable-zip",
        }
    }

    fn link_text(&self) -> &'static str {
        match self {
            ArchiveKind::TarGz => "Download .tar.gz",
            ArchiveKind::Tar => "Download .tar",
            ArchiveKind::Zip => "Download .zip",
        }
    }

    fn download_param(&self) -> &'static str {
        match self {
            ArchiveKind::TarGz => "?download=tar_gz",
            ArchiveKind::Tar => "?download=tar",
            ArchiveKind::Zip => "?download=zip",
        }
    }
}

fn fetch_index_document(
    reqwest_client: &Client,
    server: &TestServer,
    expected: StatusCode,
) -> Result<Document, Error> {
    let resp = reqwest_client.get(server.url()).send()?;
    assert_eq!(resp.status(), expected);

    Ok(Document::from_read(resp)?)
}

fn download_archive_bytes(
    reqwest_client: &Client,
    server: &TestServer,
    kind: ArchiveKind,
) -> Result<(StatusCode, usize), Error> {
    let resp = reqwest_client
        .get(server.url().join(kind.download_param())?)
        .send()?;

    Ok((resp.status(), resp.bytes()?.len()))
}

fn assert_link_presence(document: &Document, present: &[&str], absent: &[&str]) {
    let contains_text =
        |document: &Document, text: &str| document.find(Text).any(|x| x.text() == text);

    for text in present {
        assert!(
            contains_text(document, text),
            "Expected link text '{text}' to be present",
        );
    }

    for text in absent {
        assert!(
            !contains_text(document, text),
            "Expected link text '{text}' to be absent",
        );
    }
}

/// By default, all archive links are hidden.
#[rstest]
fn archives_are_disabled_links(server: TestServer, reqwest_client: Client) -> Result<(), Error> {
    let document = fetch_index_document(&reqwest_client, &server, StatusCode::OK)?;
    assert_link_presence(
        &document,
        &[],
        &[
            ArchiveKind::TarGz.link_text(),
            ArchiveKind::Tar.link_text(),
            ArchiveKind::Zip.link_text(),
        ],
    );

    Ok(())
}

/// By default, downloading archives is forbidden.
#[rstest]
#[case(ArchiveKind::TarGz)]
#[case(ArchiveKind::Tar)]
#[case(ArchiveKind::Zip)]
fn archives_are_disabled_downloads(
    #[case] kind: ArchiveKind,
    server: TestServer,
    reqwest_client: Client,
) -> Result<(), Error> {
    let (status_code, _) = download_archive_bytes(&reqwest_client, &server, kind)?;
    assert_eq!(status_code, StatusCode::FORBIDDEN);

    Ok(())
}

/// When indexing is disabled, archive links are hidden despite enabled archive options.
#[rstest]
fn archives_are_disabled_when_indexing_disabled_links(
    #[with(&["--disable-indexing", "--enable-tar-gz", "--enable-tar", "--enable-zip"])]
    server: TestServer,
    reqwest_client: Client,
) -> Result<(), Error> {
    let document = fetch_index_document(&reqwest_client, &server, StatusCode::NOT_FOUND)?;
    assert_link_presence(
        &document,
        &[],
        &[
            ArchiveKind::TarGz.link_text(),
            ArchiveKind::Tar.link_text(),
            ArchiveKind::Zip.link_text(),
        ],
    );

    Ok(())
}

/// When indexing is disabled, archive downloads are not found despite enabled archive options.
#[rstest]
#[case(ArchiveKind::TarGz)]
#[case(ArchiveKind::Tar)]
#[case(ArchiveKind::Zip)]
fn archives_are_disabled_when_indexing_disabled_downloads(
    #[case] kind: ArchiveKind,
    #[with(&["--disable-indexing", "--enable-tar-gz", "--enable-tar", "--enable-zip"])]
    server: TestServer,
    reqwest_client: Client,
) -> Result<(), Error> {
    let (status_code, _) = download_archive_bytes(&reqwest_client, &server, kind)?;
    assert_eq!(status_code, StatusCode::NOT_FOUND);

    Ok(())
}

/// Ensure the link and download to the specified archive is available and others are not
#[rstest]
#[case::tar_gz(ArchiveKind::TarGz)]
#[case::tar(ArchiveKind::Tar)]
#[case::zip(ArchiveKind::Zip)]
fn archives_links_and_downloads(
    #[case] kind: ArchiveKind,
    #[with(&[kind.server_option()])] server: TestServer,
    reqwest_client: Client,
) -> Result<(), Error> {
    let document = fetch_index_document(&reqwest_client, &server, StatusCode::OK)?;

    let (link_text, other_links, tar_gz_status, tar_status, zip_status) = match kind {
        ArchiveKind::TarGz => (
            ArchiveKind::TarGz.link_text(),
            [ArchiveKind::Tar.link_text(), ArchiveKind::Zip.link_text()],
            StatusCode::OK,
            StatusCode::FORBIDDEN,
            StatusCode::FORBIDDEN,
        ),
        ArchiveKind::Tar => (
            ArchiveKind::Tar.link_text(),
            [ArchiveKind::TarGz.link_text(), ArchiveKind::Zip.link_text()],
            StatusCode::FORBIDDEN,
            StatusCode::OK,
            StatusCode::FORBIDDEN,
        ),
        ArchiveKind::Zip => (
            ArchiveKind::Zip.link_text(),
            [ArchiveKind::TarGz.link_text(), ArchiveKind::Tar.link_text()],
            StatusCode::FORBIDDEN,
            StatusCode::FORBIDDEN,
            StatusCode::OK,
        ),
    };

    assert_link_presence(&document, &[link_text], &other_links);

    for (kind, expected) in [
        (ArchiveKind::TarGz, tar_gz_status),
        (ArchiveKind::Tar, tar_status),
        (ArchiveKind::Zip, zip_status),
    ] {
        let (status, _) = download_archive_bytes(&reqwest_client, &server, kind)?;
        assert_eq!(status, expected);
    }

    Ok(())
}

const S_IFMT: u32 = 0o170000;
const S_IFLNK: u32 = 0o120000;

/// Broken symlinks are preserved as symlink entries in archives.
#[rstest]
#[case::tar_gz(ArchiveKind::TarGz)]
#[case::tar(ArchiveKind::Tar)]
#[case::zip(ArchiveKind::Zip)]
fn archives_preserve_broken_symlinks(
    #[case] kind: ArchiveKind,
    #[with(&[ArchiveKind::TarGz.server_option(), ArchiveKind::Tar.server_option(), ArchiveKind::Zip.server_option()])]
    server: TestServer,
    reqwest_client: Client,
) -> Result<(), Error> {
    let resp = reqwest_client
        .get(server.url().join(kind.download_param())?)
        .send()?
        .error_for_status()?;

    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = resp.bytes()?.to_vec();
    match kind {
        ArchiveKind::TarGz => assert_tar_contains_broken_symlink(bytes, true)?,
        ArchiveKind::Tar => assert_tar_contains_broken_symlink(bytes, false)?,
        ArchiveKind::Zip => assert_zip_contains_broken_symlink(bytes)?,
    }

    Ok(())
}

fn assert_tar_contains_broken_symlink(bytes: Vec<u8>, gzipped: bool) -> Result<(), Error> {
    if gzipped {
        let decoder = Decoder::new(Cursor::new(bytes))?;
        assert_tar_reader_contains_broken_symlink(decoder)
    } else {
        assert_tar_reader_contains_broken_symlink(Cursor::new(bytes))
    }
}

fn assert_tar_reader_contains_broken_symlink<R: Read>(reader: R) -> Result<(), Error> {
    let mut archive = Archive::new(reader);
    let mut found = false;

    for entry in archive.entries()? {
        let entry = entry?;
        let path = entry.path()?;
        if path.as_ref().ends_with(crate::fixtures::BROKEN_SYMLINK) {
            assert_eq!(entry.header().entry_type(), EntryType::Symlink);
            let link_name = entry
                .link_name()?
                .expect("Symlink entry must have a target path");
            assert!(
                link_name
                    .as_ref()
                    .ends_with(crate::fixtures::BROKEN_SYMLINK),
                "Symlink target did not match the broken path"
            );
            found = true;
        }
    }

    assert!(
        found,
        "Archive did not contain a broken symlink entry for '{}'",
        crate::fixtures::BROKEN_SYMLINK
    );

    Ok(())
}

fn assert_zip_contains_broken_symlink(bytes: Vec<u8>) -> Result<(), Error> {
    let mut archive = ZipArchive::new(Cursor::new(bytes))?;
    let mut found = false;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if entry.name().ends_with(crate::fixtures::BROKEN_SYMLINK) {
            let mode = entry.unix_mode().unwrap_or(0);
            assert_eq!(
                mode & S_IFMT,
                S_IFLNK,
                "ZIP entry '{}' is not marked as a symlink",
                entry.name()
            );

            let mut contents = String::new();
            entry.read_to_string(&mut contents)?;
            assert!(
                contents.ends_with(crate::fixtures::BROKEN_SYMLINK),
                "Symlink target did not match the broken path"
            );

            found = true;
        }
    }

    assert!(
        found,
        "ZIP archive did not contain a broken symlink entry for '{}'",
        crate::fixtures::BROKEN_SYMLINK
    );

    Ok(())
}

/// ZIP archives store entry names using unix-style paths (no backslashes).
/// The "someDir" dir is constructed by [`fixtures`] and all items in it can be correctly processed.
#[rstest]
fn zip_archives_store_entry_name_in_unix_style(
    #[with(&["--enable-zip"])] server: TestServer,
    reqwest_client: Client,
) -> Result<(), Error> {
    let resp = reqwest_client
        .get(server.url().join("someDir/?download=zip")?)
        .send()?
        .error_for_status()?;

    assert_eq!(resp.status(), StatusCode::OK);

    let mut archive = ZipArchive::new(Cursor::new(resp.bytes()?))?;
    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        let name = entry.name();

        assert!(
            !name.contains(r"\"),
            "ZIP entry '{}' contains a backslash",
            name
        );
    }

    Ok(())
}
