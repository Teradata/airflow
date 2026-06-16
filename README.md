# Teradata Provider for Apache Airflow

This repository is Teradata's working fork of [Apache Airflow](https://github.com/apache/airflow) used to develop, test, and contribute the **Teradata provider** to the upstream Apache Airflow project.

---

## Branch Structure

| Branch | Purpose |
|--------|---------|
| `main` | Mirror of `apache/airflow:main` — kept in sync automatically. **No direct commits.** |
| `teradata-workflows` | This branch — CI/CD automation workflows only. Set as default so GitHub Actions can run them. |
| `develop` | Active development branch. All Teradata provider work starts here. |
| `feature/*` | Individual feature branches cut from `develop`. |

---

## Automated Workflows

| Workflow | Schedule | What it does |
|----------|----------|-------------|
| `teradata-sync-main-sequential.yml` | Daily 02:00 UTC + manual | Syncs `main` from `apache/airflow:main`, then pushes synced `main` to the private enterprise repo |
| `teradata-sync-feature-branches.yml` | Daily 02:30 UTC + manual | Rebases all feature branches onto `apache/airflow:main` |

To run a workflow manually: **Actions → select workflow → Run workflow**.

---

## Development Flow

```
apache/airflow:main
       ↓  (auto-sync daily)
Teradata/airflow:main
       ↓  (base for new work)
develop
       ↓
feature/<name>  →  PR to apache/airflow:main
```

1. Cut a feature branch from `develop`
2. Develop and test
3. Open a PR from `Teradata/airflow:<branch>` to `apache/airflow:main`
4. Address review comments on the same branch and push again
5. Once merged upstream, delete the feature branch

---

## Related Repositories

| Repository | URL |
|------------|-----|
| Apache Airflow (upstream) | https://github.com/apache/airflow |
| Teradata Public Fork (this repo) | https://github.com/Teradata/airflow |
| Teradata Private Enterprise Repo | https://github.com/Teradata-PE/devtools-airflow |

---

## Required GitHub Secrets

| Secret | Purpose |
|--------|---------|
| `TERADATA_PE_PAT` | Allows the main sync workflow to push synced `main` to the private enterprise repo |