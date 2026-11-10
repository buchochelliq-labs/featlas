# Releases

**In plain terms:** two things get published: the `fe-spec` command as downloadable binaries on
a GitHub release, and the `featlas` Python package on PyPI. Releasing is driven by a small
vendored tool called rab. **No release has been made yet**; nothing is published until someone
deliberately tags one.

## What is released (`rab.yml`)

| Package | Registry | Manifest | Version | Tag | Artifacts |
| --- | --- | --- | --- | --- | --- |
| `fe-spec` | GitHub release | `crates/featlas-cli/Cargo.toml` | 0.1.0 | `fe-spec-v{version}` | `rust-binary` (`fe-spec` from `featlas-cli`, with `README.md` and `LICENSE`), checksums `SHA256SUMS`, build attestation; targets `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `aarch64-apple-darwin` |
| `featlas` | PyPI | `python/pyproject.toml` | 0.1.0 | `python-v{version}` | `maturin` wheels (bindings, `featlas_databricks`, and on current sources `featlas_reverse`); targets the three above plus `x86_64-pc-windows-msvc` |

The release name comes from `fe-spec` (`release.name_from`). Each artifact has a smoke test that
runs on the built files: the binary must print its version and pass
`fe-spec test spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml --format json`;
the wheel must install and `import featlas, featlas_databricks`. The workspace crates are
`publish = false` and are not published to crates.io; they ship inside these two packages.

## The tool: rab

rab ("Release Asset Bundle") is installed under `.github/rab/` (v2.1.0 per its README) and must
not be edited; reinstall to update. CI checks it with `release.py vendored`. Commands documented
in `rab.yml`:

```text
python3 .github/rab/scripts/release.py check      CI: this file matches every manifest
python3 .github/rab/scripts/release.py sync       copy bumped versions in from the manifests
python3 .github/rab/scripts/release.py status     what is published, tagged or due
python3 .github/rab/scripts/release.py tag        tag the release on main and wait for its run
```

`check` on this commit:

```text
warning: library crates rab.yml does not publish: featlas-adapters (publish = false), featlas-api (publish = false), featlas-core (publish = false). To publish one, list it with registry: crates.io; a crate meant to stay private needs nothing
rab.yml matches the repository (release 0.1.0).
```

`tag` accepts `--commit`, `--release`, `--remote`, `--timeout` and `--dry-run`.

## The release workflow (`.github/workflows/release.yml`)

Triggered by a pushed `release-*` tag, or manually with an existing tag (and optional
`verify_only` for a run that failed after uploading). One release at a time, never cancelled.

1. **gate** — checks the tag once and decides what is due.
2. **ci** — runs `ci.yml` on the tagged commit (via `workflow_call` with `ref`).
3. **build** — `rab-build.yml`: one job per artifact and target, each running its smoke test.
4. **publish-github** — uploads the binaries with build provenance (`contents: write`,
   `id-token: write`, `attestations: write`).
5. **publish-pypi** — environment `pypi`, OIDC (`id-token: write`), trusted publishing.
6. **record** — always runs; writes `release-record.json` (release, commit, each package's
   outcome, files and registry URL) as the artifact `release-record`.

Third-party actions in this workflow are pinned to commits.

## Cutting a release

From `README.md` and `rab.yml`:

1. Make sure the `pypi` environment's trusted publisher is configured on PyPI and in the
   repository (required before the first PyPI upload).
2. Bump versions in the manifests (`crates/featlas-cli/Cargo.toml` via the workspace version,
   `python/pyproject.toml`), run `release.py sync` and `release.py check`, and merge to `main`.
3. On `main`, run `python3 .github/rab/scripts/release.py tag` (try `--dry-run` first). It pushes
   the tags and waits for the release run.
4. Inspect the `release-record` artifact of the run.

The live Databricks evidence is not a release gate in these workflows; the capability reports a
released binary produces depend on the adapter lock it was built with
([Capabilities](concepts/capabilities.md#the-databricks-adapter-lock)).
