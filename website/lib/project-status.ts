export type PublishedRelease = {
  tag: string;
  version: string;
};

const configuredReleaseVersion = process.env.AQL_RELEASE_VERSION?.trim();

if (configuredReleaseVersion && !/^\d+\.\d+\.\d+$/.test(configuredReleaseVersion)) {
  throw new Error("AQL_RELEASE_VERSION must use x.y.z without a leading v");
}

export const publishedRelease: PublishedRelease | null =
  configuredReleaseVersion
    ? {
        tag: `v${configuredReleaseVersion}`,
        version: configuredReleaseVersion,
      }
    : null;

export const prebuiltPlatforms = [
  "macOS arm64",
  "macOS x86_64",
  "Linux arm64",
  "Linux x86_64",
];
