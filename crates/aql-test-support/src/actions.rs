use std::fs;
use std::path::Path;

use crate::{TestResult, reset_output, write_private};

pub fn generate_actions(output: &Path) -> TestResult {
    reset_output(output)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(output, fs::Permissions::from_mode(0o700))?;
    }
    write_private(
        &output.join("SYNTHETIC_ACTION_CHANNEL"),
        b"aql-synthetic-action-channel-v1\n",
    )?;
    write_private(
        &output.join("channel.json"),
        b"{\"schema_version\":\"aql-synthetic-official-channel-v1\",\"source_id\":\"synthetic-source-opaque\",\"entities\":{\"synthetic-entity-opaque\":{\"revision\":1,\"archived\":false,\"title\":\"Synthetic original title\",\"external_effects\":0}},\"outcomes\":{}}\n",
    )?;
    Ok(())
}
