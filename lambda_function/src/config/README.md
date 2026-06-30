# Baked package configuration

This directory holds **read-only, build-time configuration files** that ship
inside the Lambda image. Each immediate subdirectory corresponds to one
upstream Python package whose import-time behavior we need to control before
any handler code runs.

```
lambda_function/src/config/
├── README.md          <- this file
└── <package>/         <- one directory per package needing baked config
    └── config.yml     <- (or whatever filename the package expects)
```

At build time, the existing `COPY src/. ${FUNCTION_DIR}` step in
`lambda_function/Dockerfile` ships this tree to `/lambda_function/config/`
inside the image, and the recursive `chmod -R 755` / `chown -R 1000:1000`
steps already apply correct permissions.

## Why bake config into the image?

Some packages (e.g. `ccsdspy`) run a config loader and logger init at
**package import time**. If their default config asks for a writable path
(log files, cache dirs) on a read-only filesystem, the very first
`import <package>` blows up before our handler can do anything about it.

Pointing the package at a baked-in `config.yml` via an environment variable
(see "Dockerfile wiring" below) lets us pre-empt the bad default without
patching the package or shimming the handler.

## AWS Lambda runtime filesystem constraint

> **The entire image filesystem is read-only at Lambda runtime. Only `/tmp`
> is writable.**

That means **nothing under `/lambda_function/config/<pkg>/` may be written
to at runtime**. If a package needs to write log files, caches, or any
other state, the config you bake here **must** point those writes at a
path under `/tmp/<pkg>/`. Two corollaries:

1. The caller (handler module, entry script, or — preferred — the package
   itself, via an upstream fix) is responsible for `mkdir -p /tmp/<pkg>`
   on cold start. `/tmp` is a fresh empty mount on each execution
   environment, so the directory will not survive across cold starts.
2. Lambda's `/tmp` is wiped between execution environments, so any log
   files written there are ephemeral. If you want durable, queryable logs,
   prefer disabling file logging entirely and rely on Lambda's
   stdout/stderr → CloudWatch pipeline (which is what we do for `ccsdspy`
   below).

## Dockerfile wiring

For each package with a directory here, add **one** `ENV` line to
`lambda_function/Dockerfile` in the post-COPY env block (after
`RUN chown -R 1000:1000 ${FUNCTION_DIR}` and before
`# Copy entry script into function director`):

```dockerfile
# Package config locations (baked under /lambda_function/config/<pkg>/)
ENV ccsdspy_CONFIGDIR=/lambda_function/config/ccsdspy
# ENV <pkg>_CONFIGDIR=/lambda_function/config/<pkg>   <- pattern for future packages
```

Notes:

- Use `ENV`, **not** `ARG`. `ARG` values are build-time only and are not
  visible to the Python runtime via `os.environ`, which is what these
  packages consult at import time.
- The exact variable name (`ccsdspy_CONFIGDIR`, `SOMETHING_CONFIG_DIR`,
  etc.) is dictated by the upstream package — match whatever it reads.
- Place the `ENV` lines together as a single block so they are easy to
  grep and review.

## Worked example: `ccsdspy`

**Problem.** Recent `ccsdspy` releases unconditionally open a
`logging.FileHandler` against a relative `log_file_path` at package
import time. In the Lambda image the CWD is `/lambda_function/` (which
is read-only at runtime), so `import ccsdspy` — pulled in transitively
by `padre_meddea.io.file_tools` — raises:

```
OSError: [Errno 30] Read-only file system: '/lambda_function/ccsdspy.log'
```

**Fix.** Bake a minimal `config.yml` that disables file logging, and
point `ccsdspy_CONFIGDIR` at it:

`lambda_function/src/config/ccsdspy/config.yml`:

```yaml
logger:
  log_to_file: false
```

`lambda_function/Dockerfile` (in the post-COPY env block):

```dockerfile
ENV ccsdspy_CONFIGDIR=/lambda_function/config/ccsdspy
```

`ccsdspy.config.load_config()` finds our YAML first and skips the
packaged default, so `_init_log` never reaches the `FileHandler` branch.
The `StreamHandler` still attaches, and log records continue to flow to
CloudWatch via stdout.

If you ever need `ccsdspy` to write a real log file, change the YAML to:

```yaml
logger:
  log_to_file: true
  log_file_path: /tmp/ccsdspy/ccsdspy.log
```

…and ensure `/tmp/ccsdspy` is created on cold start (in
`entry_script.sh` or at the very top of `lambda.py`, before any
`import ccsdspy`). Do **not** point `log_file_path` at anything under
`/lambda_function/`.

## Adding a new package

1. `mkdir lambda_function/src/config/<pkg>/` and drop the config file(s)
   the package expects.
2. If the package needs writable scratch space, make sure the baked
   config points it at `/tmp/<pkg>/...`, never inside `config/`.
3. Add one `ENV <pkg>_CONFIGDIR=/lambda_function/config/<pkg>` line to
   the post-COPY env block in `lambda_function/Dockerfile`.
4. If `/tmp/<pkg>/` needs to exist before the package is imported, add a
   `mkdir -p /tmp/<pkg>` to `entry_script.sh` (preferred over runtime
   Python so it happens before the Python interpreter starts).
