# tap-restaurant365

`tap-restaurant365` is a Singer tap for Restaurant365.


<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install tap-restaurant365
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-restaurant365.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-restaurant365 --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-restaurant365 --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage


### Executing the Tap Directly

```bash
tap-restaurant365 --version
tap-restaurant365 --help
tap-restaurant365 --config CONFIG --discover > ./catalog.json

tap-restaurant365 --config CONFIG --catalog CATALOG > data.txt
```

## Developer Resources


```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-restaurant365` CLI interface directly using `poetry run`:

```bash
poetry run tap-restaurant365 --help
```

