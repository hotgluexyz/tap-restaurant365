# tap-restaurant365

`tap-restaurant365` is a Singer tap for Restaurant365, built with `hotglue-singer-sdk`.

## Configuration

Create a config file with your Restaurant365 credentials:

```json
{
  "username": "USERNAME",
  "password": "PASSWORD",
  "store_name": "YOUR_SUBDOMAIN",
  "start_date": "2021-10-01T00:00:00Z"
}
```

`store_name` is the subdomain from `https://YOUR_SUBDOMAIN.restaurant365.com`.

A full list of supported settings and capabilities is available by running:

```bash
tap-restaurant365 --about
```

## Usage

Discover streams and run a sync:

```bash
tap-restaurant365 --config config.json --discover > catalog.json
tap-restaurant365 --config config.json --catalog selected-catalog.json --state state.json
```

During development, use Poetry:

```bash
poetry run tap-restaurant365 --config config.json --discover > catalog.json
```

### Available and selected filters

The tap supports Hotglue filter discovery and runtime filter selection via `--get-available-filters` and `--selected-filters`.

The `bills` stream can be filtered by vendor `companyId`. Vendor options are loaded from the Restaurant365 `/Company` endpoint.

Get available filters:

```bash
tap-restaurant365 \
  --config config.json \
  --catalog catalog.json \
  --get-available-filters > available-filters.json
```

Run a sync with selected filters:

```bash
tap-restaurant365 \
  --config config.json \
  --catalog catalog.json \
  --state state.json \
  --selected-filters selected-filters.json
```

Example `selected-filters.json` for a single vendor (`EQ`):

```json
{
  "filters_version": "1.0.0",
  "streams": {
    "bills": {
      "clause_1": {
        "field": "vendors",
        "operator": "EQ",
        "value": "Acme Corp (12345)"
      }
    }
  }
}
```

Example for multiple vendors (`IN`):

```json
{
  "filters_version": "1.0.0",
  "streams": {
    "bills": {
      "clause_1": {
        "field": "vendors",
        "operator": "IN",
        "value": [
          "Acme Corp (12345)",
          "Other Vendor (67890)"
        ]
      }
    }
  }
}
```

Filter values use the `Vendor Name (companyId)` format returned in `available-filters.json`.

## Developer Resources

```bash
pipx install poetry
poetry install
```

Run tests:

```bash
poetry run pytest
```

Test the CLI:

```bash
poetry run tap-restaurant365 --help
```
