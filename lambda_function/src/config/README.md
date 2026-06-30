# Baked package configuration

This directory holds **source-of-truth, build-time configuration files** for
upstream Python packages whose import-time behavior we need to control before
any handler code runs. Each immediate subdirectory corresponds to one package.

```
lambda_function/src/config/
├── README.md          <- this file
└── <package>/         <- one directory per package needing baked config
    └── config.yml     <- (or whatever filename the package expects)
```

## Three-stage path: repo → image → `/tmp`

Config files live in this directory in the repo (versioned, reviewed). At
build time, the existing `COPY src/. ${FUNCTION_DIR}` step in
`lambda_function/Dockerfile` ships the tree to `/lambda_function/config/`
inside the image, and the recursive `chmod -R 755` / `chown -R 1000:1000`
steps apply correct permissions. At runtime, `lambda_function/entry_script.sh`
mirrors `/lambda_function/config/` into the writable `/tmp/config/` *before*
the Python runtime starts, and the Dockerfile `ENV` lines point each package
at the `/tmp/config/<pkg>/` mirror.

```
repo:        lambda_function/src/config/<pkg>/...
              └─ COPY src/. ${FUNCTION_DIR}  (build time)
image:       /lambda_function/config/<pkg>/...    (read-only at runtime)
              └─ cp -R /lambda_function/config/. /tmp/config/  (cold start, entry_script.sh)
runtime:     /tmp/config/<pkg>/...                 (writable, what the ENV var points at)
```

## Why bake config into the image?

Some packages (e.g. `ccsdspy`) run a config loader and logger init at
**package import time**. If the default config asks for a writable path on a
read-only filesystem, or simply requires `<pkg>_CONFIGDIR` itself to be
writable, the very first `import <package>` blows up before our handler can
do anything about it. Pointing the package at a pre-seeded, writable
`/tmp/config/<pkg>/` lets us pre-empt the bad default without patching the
package or shimming the handler.

## AWS Lambda runtime filesystem constraint

> **The entire image filesystem is read-only at Lambda runtime. Only `/tmp`
> is writable.**

Consequences:

1. **`/lambda_function/config/<pkg>/` cannot be the env-var target** for any
   package that requires its config dir to be writable (e.g. `ccsdspy`'s
   `_get_user_configdir()` calls `mkdir` + `os.access(W_OK)` *before* it
   reads the YAML). That is why `entry_script.sh` mirrors the tree into
   `/tmp/config/` on every invocation, and why the `ENV` lines point at the
   `/tmp` mirror.
2. **`/tmp` is wiped between execution environments**, so the seed step has
   to re-run on every cold start. The `cp -R` is idempotent for warm starts.
3. **Log files, caches, and other runtime state** for a baked-config package
   may live alongside its config under `/tmp/config/<pkg>/` — the tree is
   writable. They are ephemeral (gone on the next cold start), so for
   anything durable prefer Lambda's stdout/stderr → CloudWatch pipeline.

## Dockerfile wiring

For each package with a directory here, add **one** `ENV` line to
`lambda_function/Dockerfile` in the post-COPY env block (after
`RUN chown -R 1000:1000 ${FUNCTION_DIR}` and before
`# Copy entry script into function director`):

```dockerfile
# Package config locations. Source-of-truth YAML lives in src/config/<pkg>/
# (baked into the image at ${FUNCTION_DIR}config/<pkg>/) and is mirrored to
# the writable /tmp/config/<pkg>/ at runtime by entry_script.sh.
ENV ccsdspy_CONFIGDIR=/tmp/config/ccsdspy
# ENV <pkg>_CONFIGDIR=/tmp/config/<pkg>   <- pattern for future packages
```

Notes:

- The env target is `/tmp/config/<pkg>`, **not** `${FUNCTION_DIR}config/<pkg>`,
  because some packages require the dir to be writable. The seed step in
  `entry_script.sh` is what makes `/tmp/config/<pkg>` exist.
- Use `ENV`, **not** `ARG`. `ARG` values are build-time only and are not
  visible to the Python runtime via `os.environ`.
- The exact variable name (`ccsdspy_CONFIGDIR`, `SOMETHING_CONFIG_DIR`, etc.)
  is dictated by the upstream package — match whatever it reads.
- Place the `ENV` lines together as a single block so they are easy to grep.

## Worked example: `ccsdspy`

**Problem.** Recent `ccsdspy` releases (a) unconditionally open a
`logging.FileHandler` against a relative `log_file_path` at package import
time, and (b) require `ccsdspy_CONFIGDIR` itself to be writable
(`_get_user_configdir()` does `mkdir(...exist_ok=True)` + `os.access(W_OK)`
before reading `config.yml`). In the Lambda image both fail: CWD
`/lambda_function/` is read-only at runtime, and so is any baked subdir.
`import ccsdspy` — pulled in transitively by `padre_meddea.io.file_tools` —
raises one of:

```
OSError: [Errno 30] Read-only file system: '/lambda_function/ccsdspy.log'
RuntimeError: Could not write to ccsdspy_CONFIGDIR="/lambda_function/config/ccsdspy"
```

**Fix.** Bake a `config.yml` that disables file logging, seed it into a
writable `/tmp/config/ccsdspy/` at cold start, and point `ccsdspy_CONFIGDIR`
at the writable mirror:

`lambda_function/src/config/ccsdspy/config.yml` (the relevant key):

```yaml
logger:
  log_to_file: false
```

`lambda_function/entry_script.sh` (added near the top):

```sh
if [ -d "/lambda_function/config" ]; then
    mkdir -p /tmp/config
    cp -R /lambda_function/config/. /tmp/config/
fi
```

`lambda_function/Dockerfile` (in the post-COPY env block):

```dockerfile
ENV ccsdspy_CONFIGDIR=/tmp/config/ccsdspy
```

`ccsdspy._get_user_configdir()` now sees a writable `/tmp/config/ccsdspy/`
seeded with our YAML, `load_config()` reads it instead of the packaged
default, and `_init_log` never reaches the `FileHandler` branch. The
`StreamHandler` still attaches, so log records continue to flow to CloudWatch
via stdout.

If you ever need `ccsdspy` to write a real log file, change the YAML to:

```yaml
logger:
  log_to_file: true
  log_file_path: /tmp/config/ccsdspy/ccsdspy.log
```

No extra `mkdir` is needed — the seed step already created
`/tmp/config/ccsdspy/`. The file is ephemeral (wiped per execution
environment), so for durable logs stick with the stdout/CloudWatch route.

## Adding a new package

1. `mkdir lambda_function/src/config/<pkg>/` and drop the config file(s)
   the package expects. The existing `COPY src/. ${FUNCTION_DIR}` step ships
   it; no Dockerfile COPY/chmod edits needed.
2. If the baked config refers to any writable runtime path (log file, cache),
   point it at `/tmp/config/<pkg>/...`. The `entry_script.sh` seed step
   already guarantees that directory exists and is writable.
3. Add one `ENV <pkg>_CONFIGDIR=/tmp/config/<pkg>` line to the post-COPY env
   block in `lambda_function/Dockerfile` (match whatever env var name the
   package actually reads).
4. No `entry_script.sh` change is needed — the generic seed step already
   mirrors the entire `/lambda_function/config/` tree.
