# Running Teradata System Tests Locally

Steps to run the Teradata provider system tests against a local Teradata VM instance,
without requiring a ClearScape Analytics Engine (CSAE) environment.

---

## Prerequisites (one-time setup)

### 1. Switch to the branch you want to test

```bash
cd ~/devtools/airflow
git checkout main   # or develop, or your feature branch
```

> **Note:** Do NOT run from `teradata-workflows` — that branch contains only CI workflow
> files. The provider source code and system tests live on `main`/`develop`.

### 2. Install system build dependencies

Required for compiling native extensions (pykerberos, xmlsec, etc.):

```bash
sudo apt-get update -q
sudo apt-get install -y libkrb5-dev krb5-multidev gcc g++ \
  libsasl2-dev libldap2-dev libssl-dev libffi-dev libgeos-dev \
  unixodbc-dev default-libmysqlclient-dev libxmlsec1-dev
```

### 3. Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

### 4. Install Airflow with Teradata provider and test dependencies

```bash
uv sync --extra teradata
uv sync --project providers/teradata --group dev
```

---

## Configure environment variables

Replace the placeholder values with your local Teradata VM details:

```bash
export AIRFLOW_HOME=~/airflow
mkdir -p $AIRFLOW_HOME

# --- Set these to match your local Teradata VM ---
export TERADATA_HOST=<your-vm-ip-or-hostname>
export TERADATA_USER=dbc
export TERADATA_PASSWORD=dbc
export TERADATA_DATABASE=dbc
# -------------------------------------------------

export AIRFLOW_CONN_TERADATA_DEFAULT="teradata://${TERADATA_USER}:${TERADATA_PASSWORD}@${TERADATA_HOST}/${TERADATA_DATABASE}"
export AIRFLOW_CONN_TERADATA_SP_CALL="teradata://${TERADATA_USER}:${TERADATA_PASSWORD}@${TERADATA_HOST}/${TERADATA_DATABASE}?tmode=TERA"
export AIRFLOW_CONN_TERADATA_SSL_DEFAULT="teradata://${TERADATA_USER}:${TERADATA_PASSWORD}@${TERADATA_HOST}/${TERADATA_DATABASE}?sslmode=allow"

# Unique ID per run — prevents DAG ID collisions if multiple runs overlap
export SYSTEM_TESTS_ENV_ID=teradatasystemtest-$(date +%s)
```

---

## Run the tests

### Run a single test first to verify the setup

```bash
uv run pytest --system -xvs \
  providers/teradata/tests/system/teradata/example_teradata.py
```

### Run all tests that work with a local Teradata instance

```bash
uv run pytest --system --junitxml=report_test.xml -xvs \
  providers/teradata/tests/system/teradata/example_teradata.py \
  providers/teradata/tests/system/teradata/example_teradata_to_teradata_transfer.py \
  providers/teradata/tests/system/teradata/example_teradata_call_sp.py \
  providers/teradata/tests/system/teradata/example_ssl_teradata.py \
  providers/teradata/tests/system/teradata/example_tpt.py \
  providers/teradata/tests/system/teradata/example_bteq.py
```

### Tests to skip for local-only runs

These tests require external services not available on a local VM:

| Test file | Requires |
|---|---|
| `example_azure_blob_to_teradata_transfer.py` | Azure Blob Storage credentials (`wasb_default` connection) |
| `example_s3_to_teradata_transfer.py` | AWS credentials (`aws_default` connection) |
| `example_remote_bteq.py` | SSH connection to remote host (`ssh_default`) |
| `example_remote_tpt.py` | SSH connection to remote host (`ssh_default`) |
| `example_teradata_compute_cluster.py` | VantageCloud Lake instance (`teradata_lake` connection) |

---

## Generate the dashboard report locally

After the tests finish, generate the HTML report to verify it looks correct before pushing:

```bash
# Copy report scripts to working directory (run from repo root)
cp .github/workflows/system-tests-files/report_parse_xml.py .
cp .github/workflows/system-tests-files/report_generate_index_html.py .
cp .github/workflows/system-tests-files/report_index_template.html .
cp .github/workflows/system-tests-files/reporttest.csv . 2>/dev/null || touch reporttest.csv

pip3 install --quiet jinja2

# Parse results from pytest XML output into the CSV history
python3 report_parse_xml.py

# Generate the HTML dashboard
python3 report_generate_index_html.py

# Open index.html in a browser to verify icons and status message look correct
```

---

## Push changes after local validation

Once tests pass locally and the report looks correct, push your branch and trigger the
workflow against the real CI environment:

```bash
git push origin <your-branch>
```

Then go to **Actions → airflow-teradata-main-systemtest-dashboard → Run workflow** on
`github.com/Teradata/airflow` and trigger with `skip_csae=false`.
