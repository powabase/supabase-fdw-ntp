# GitHub Actions Workflows

This directory contains automated CI/CD workflows for the NTP Energy Market WASM FDW project, implementing 2025 best practices for Rust WebAssembly releases.

## Workflows

### 1. `release.yml` - Production Release Automation

**Trigger:** Push tags matching `v*` (e.g., `v0.1.0`)

**Purpose:** Automatically builds, validates, and releases WASM binaries to GitHub Releases

**Steps:**
1. ✅ Install Rust toolchain with `wasm32-unknown-unknown` target
2. ✅ Cache Cargo dependencies (saves 2-5 minutes on subsequent runs)
3. ✅ Install `cargo-component` via pre-built binary (fast)
4. ✅ Build optimized WASM binary
5. ✅ **Validate WASM structure** (prevents WASI import bugs)
6. ✅ Check binary size (warns if > 300 KB, expected: ~282 KB)
7. ✅ Calculate SHA256 checksum
8. ✅ Create GitHub Release with comprehensive notes
9. ✅ Upload 3 files: `.wasm`, `.sha256`, `checksums.txt`

**Build Time:**
- First run: ~5-7 minutes
- Cached runs: ~1-2 minutes ⚡

**Usage:**
```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

---

### 2. `test.yml` - Continuous Integration

**Trigger:** Push to `main` or `develop`, Pull Requests

**Purpose:** Validate code quality and WASM builds before merging

**Jobs:**

#### Lint & Format
- Runs `cargo fmt --check` (formatting)
- Runs `cargo clippy` (linting)
- Fails on warnings

#### Run Unit Tests
- Executes `cargo test --lib`
- Validates 119 unit tests pass
- Tests transformations, parsers, routing, error handling

#### Build & Validate
- Builds WASM in debug and release modes
- Validates WASM structure
- Checks for WASI CLI imports (must be zero)
- Verifies binary size < 500 KB
- Uploads WASM artifact for inspection

**Build Time:**
- First run: ~3-4 minutes
- Cached runs: ~30-60 seconds

---

## Key Features

### 2025 Best Practices

**Modern Actions:**
- `dtolnay/rust-toolchain@stable` - Maintained Rust toolchain (replaces deprecated `actions-rs/toolchain`)
- `Swatinem/rust-cache@v2` - Intelligent Cargo caching
- `taiki-e/install-action@v2` - Fast binary installations
- `softprops/action-gh-release@v2` - Latest release action

**Security & Validation:**
- Automated WASI import detection (prevents #1 deployment bug)
- Binary size monitoring
- SHA256 checksum generation
- Release asset integrity

**Performance:**
- Intelligent caching (Cargo registry, git deps, build artifacts)
- Pre-built binaries for tools (cargo-component)
- Parallel job execution where possible

---

## WASM Validation

### Critical Check: Zero WASI Imports

The workflow **automatically validates** that the WASM binary has **zero WASI CLI imports**. This prevents the most common deployment issue:

```bash
# What the workflow checks
wasm-tools component wit supabase_fdw_ntp.wasm | grep wasi:cli
# Expected: (no output)
```

**Why this matters:**
- Supabase Wrappers doesn't provide WASI CLI interfaces
- Using `wasm32-wasip1` target causes this error:
  ```
  component imports instance 'wasi:cli/environment@0.2.0',
  but a matching implementation was not found
  ```
- Workflow FAILS if any WASI CLI imports detected
- Saves hours of debugging deployment issues

### Expected WASM Imports

The binary should ONLY import Supabase Wrappers interfaces:

```wit
world root {
  import supabase:wrappers/http@0.2.0;
  import supabase:wrappers/stats@0.2.0;
  import supabase:wrappers/time@0.2.0;
  import supabase:wrappers/utils@0.2.0;
  export supabase:wrappers/routines@0.2.0;
}
```

---

## Release Process

### Standard Release

```bash
# 1. Ensure all changes committed
git add .
git commit -m "Release v0.1.0: Description"
git push origin main

# 2. Create and push tag
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0

# 3. Monitor workflow
# Visit: https://github.com/powabase/supabase-fdw-ntp/actions
```

### Pre-Release Testing (Recommended)

Test with a release candidate first:

```bash
# 1. Create RC tag
git tag -a v0.1.0-rc1 -m "Release candidate v0.1.0-rc1"
git push origin v0.1.0-rc1

# 2. Workflow runs, creates pre-release
# 3. Test the binary in Supabase

# 4. If successful, create final release
git tag -d v0.1.0-rc1                  # Delete local
git push origin :refs/tags/v0.1.0-rc1  # Delete remote
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

---

## Release Notes Format

The workflow auto-generates comprehensive release notes:

```markdown
## NTP Energy Market WASM FDW v0.2.0

**Binary Size:** 301 KB
**SHA256:** `494038bc7b5ed52880a2d9e276bb85adb7c8b91794f6bbfbba9ec147467297f2`
**Target:** `wasm32-unknown-unknown` (bare WASM, no WASI)
**Supabase Wrappers:** Compatible with v0.2.0+

### Tables Included
1. renewable_energy_timeseries - Solar and wind generation (9 endpoints, 10 columns)
2. electricity_market_prices - Spot market, premiums, negative flags (4 endpoints, 5 columns)
3. redispatch_events - Grid intervention events (1 endpoint, 9 columns)
4. grid_status_timeseries - Minute-by-minute stability monitoring (1 endpoint, 3 columns)

### Features
- OAuth2 authentication with token caching
- 11 ETL transformations (German locale support)
- Query routing (WHERE → API endpoints)
- 119 unit tests, security hardened
- JOIN support validated

### Installation (Supabase)
[SQL code for deployment]

### Quick Test
[Example queries]
```

---

## Troubleshooting

### Workflow Fails: WASI Import Detected

**Error:**
```
❌ ERROR: Found 5 WASI CLI imports!
This means the build uses wasm32-wasip1 instead of wasm32-unknown-unknown
```

**Fix:**
- Verify `Cargo.toml` doesn't specify wasip1 target
- Check that `cargo component build` uses `--target wasm32-unknown-unknown`
- Rebuild locally and test: `wasm-tools component wit target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm | grep wasi:cli`

### Workflow Slow

**Expected:**
- First run: 5-7 minutes
- Cached runs: 1-2 minutes

**If slower:**
- Check if cache is working: Look for "Cache restored" in logs
- Verify `Swatinem/rust-cache@v2` step succeeds
- Consider cache key conflicts (cleared automatically)

### Binary Too Large

**Warning:**
```
⚠️ WARNING: Binary size (350 KB) is larger than expected
```

**Check:**
- Verify `Cargo.toml` has `[profile.release]` optimizations:
  ```toml
  [profile.release]
  opt-level = "z"       # Size optimization
  lto = true            # Link-time optimization
  strip = "debuginfo"   # Strip debug info
  codegen-units = 1     # Better optimization
  ```

### Unit Tests Failing

**Error:**
```
❌ ERROR: test failed
```

**Debug:**
- Run tests locally: `cargo test --lib`
- Check test output for specific failures
- Verify test fixtures in `tests/` directory
- Common issues:
  - Transformation tests (38 tests)
  - CSV parser tests (31 tests)
  - Query router tests (29 tests)

---

## Caching Strategy

The workflows use `Swatinem/rust-cache@v2` which caches:

1. **Cargo registry** (`~/.cargo/registry`)
2. **Git dependencies** (`~/.cargo/git`)
3. **Build artifacts** (`target/`)

**Cache keys:**
- `wasm-release` - Release workflow cache
- `wasm-test` - Test workflow cache

**Benefits:**
- 2-5 minutes saved per run
- Shared across workflow runs
- Automatically cleaned if stale

---

## Local Testing (Optional)

Test workflows locally with [`act`](https://github.com/nektos/act):

```bash
# Install act
brew install act

# List workflows
act -l

# Dry run release workflow
act push -n --eventpath event.json

# Run test workflow
act pull_request
```

**Note:** Some features (GitHub token, releases) won't work locally.

---

## Permissions

Both workflows require:

```yaml
permissions:
  contents: write  # Create releases, upload assets
```

**Repository Settings:**
- Settings → Actions → General → Workflow permissions
- Select: "Read and write permissions" ✅

---

## NTP-Specific Configuration

### Binary Specifications
- **Name:** `supabase_fdw_ntp.wasm`
- **Expected Size:** ~282 KB
- **Warning Threshold:** > 300 KB
- **Failure Threshold:** > 500 KB

### API Integration
- **Base URL:** `https://ds.netztransparenz.de`
- **OAuth2 URL:** `https://identity.netztransparenz.de/users/connect/token`
- **Scope:** `ntpStatistic.read_all_public`
- **Authentication:** OAuth2 client credentials flow

### Data Coverage
- **Geographic:** Germany only (4 TSO zones: 50Hertz, Amprion, TenneT, TransnetBW)
- **Endpoints:** 15 API endpoints
- **Tables:** 4 consolidated foreign tables
- **Data Types:** CSV (14 endpoints), JSON (1 endpoint - TrafficLight)

---

## Future Enhancements

### Optional Features (Not Yet Implemented)

1. **SLSA Provenance** - Supply chain security
   ```yaml
   uses: slsa-framework/slsa-github-generator/.github/workflows/generator_generic_slsa3.yml@v2.0.0
   ```

2. **Artifact Attestations** - GitHub native provenance
   ```yaml
   uses: actions/attest-build-provenance@v1
   ```

3. **Multi-Platform Builds** - Cross-compilation
   ```yaml
   strategy:
     matrix:
       target: [wasm32-unknown-unknown, wasm32-wasip2]
   ```

4. **Release Notifications** - Slack/Discord webhooks

5. **Automated E2E Testing** - Deploy to test Supabase instance with OAuth2 credentials

6. **Performance Benchmarks** - Track binary size and build time trends

---

## References

### GitHub Actions
- **GitHub Actions Documentation:** https://docs.github.com/en/actions
- **Rust Toolchain Action:** https://github.com/dtolnay/rust-toolchain
- **Rust Cache Action:** https://github.com/Swatinem/rust-cache
- **Install Action:** https://github.com/taiki-e/install-action
- **Release Action:** https://github.com/softprops/action-gh-release

### Tools
- **wasm-tools:** https://github.com/bytecodealliance/wasm-tools
- **cargo-component:** https://github.com/bytecodealliance/cargo-component
- **Supabase Wrappers:** https://github.com/supabase/wrappers

### NTP Project Docs
- **README:** Project overview and quick start
- **QUICKSTART:** 5-minute setup guide
- **CLAUDE:** Complete development guide with all patterns
- **Endpoint Docs:** docs/endpoints/*.md
- **Architecture:** docs/reference/ARCHITECTURE.md (15 ADRs)

---

**Last Updated:** October 25, 2025
**Workflow Version:** v0.1.0
**Maintainer:** Powabase Team
