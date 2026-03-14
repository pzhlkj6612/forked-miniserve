use reqwest::blocking::Client;
use rstest::rstest;
use select::predicate::Attr;

mod fixtures;
mod utils;

use crate::fixtures::{Error, TestServer, reqwest_client, server};
use crate::utils::document_from_read;

// There are few tests here because the pastebin is implemented by converting a textareas content
// into an in-memory blob/file, and adding that file to the existing file upload form. We can't
// test the JS here, and any testing the actual "upload" would just be retesting the existing
// uploader.

#[rstest]
#[case::without_flag(&["--upload-files"], false)]
#[case::with_flag(&["--upload-files", "--pastebin"], true)]
fn paste_entry_only_appears_with_flag(
    #[case] _flags: &[&str],
    #[case] should_exist: bool,
    #[with(_flags)] server: TestServer,
    reqwest_client: Client,
) -> Result<(), Error> {
    let body = reqwest_client
        .get(server.url())
        .send()?
        .error_for_status()?;
    let parsed = document_from_read(body)?;
    let exists = parsed.find(Attr("id", "pastebin")).next().is_some();

    assert_eq!(
        exists, should_exist,
        "Expected exists(#pastebin) to return {}, but got {}",
        should_exist, exists
    );

    Ok(())
}
