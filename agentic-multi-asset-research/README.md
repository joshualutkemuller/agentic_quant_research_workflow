# Agentic Multi-Asset Research Workflow

This repository orchestrates an agentic research workflow for multi-asset portfolios and dashboards.

## Overview

The system is organized into five logical agents:

1. **Data & Pipeline Agent**  
   Maintains cross-asset benchmark and factor data, standardizes return calculations, and monitors data quality.

2. **Research & Modeling Agent**  
   Runs factor-timing models, risk and volatility analytics, and strategic asset allocation optimizers.

3. **Insight & Narrative Agent**  
   Translates model output into structured commentary and decision-support narratives.

4. **Dashboard Agent**  
   Publishes pre-shaped JSON/CSV feeds consumed by front-end dashboards (e.g. Power BI or React-based UIs).

5. **GitHub Steward Agent**  
   Integrates with GitHub to create issues, pull requests, and changelog entries when new reports, diagnostics, or configuration changes are produced.

The workflow is driven by configuration files in `config/` and orchestrated through Python pipelines in `src/pipelines/`, triggered on schedules defined in GitHub Actions.

### End-to-end workflow summary

- **Data/benchmarks** – standardize and validate benchmark, factor, and pricing data using `config/datasources.yaml` with quality and recency checks.
- **Research/analytics** – run factor-timing, risk, and asset-allocation models configured in `config/models.yaml`, producing structured portfolio diagnostics.
- **Narrative/insights** – convert analytics into Markdown reports placed in `reports/` and pre-shaped data feeds stored in `dashboards/data/` for downstream dashboards.
- **Consumer app blueprint** – generate a retail investing playbook via the `consumer` pipeline, combining portfolio analytics with iOS/web/backend delivery plans and action items.
- **Snowflake analytics blueprint** – ingest a Snowflake schema via the `snowflake` pipeline to emit table metadata, ready-to-run SQL queries, and dashboard starters for benchmark/constituent analysis.
- **Orchestration & GitHub** – pipelines in `src/pipelines/` are invoked through `src.orchestration.runner` (manually or via CI), while the GitHub steward agent opens issues/PRs when new artifacts are produced.

### Consumer quant blueprint (new)

In addition to the institutional research stack, the repository now ships a consumer-focused agentic framework that assembles a turnkey blueprint for retail investing apps. It ingests a sample household portfolio from `config/consumer/blueprint.yaml`, runs allocation, concentration, stress, and cashflow-aware growth analytics, and produces a Markdown blueprint describing the data connectors, analytics modules, and actionable playbooks needed for an “invest like a quant” consumer experience.

## Key Components

- `config/datasources.yaml` – definitions of data sources, benchmark universes, and recency thresholds  
- `config/models.yaml` – factor-timing, risk, and SAA model configurations  
- `config/dashboards.yaml` – dashboard tiles and data feed definitions  
- `src/agents/` – agent implementations  
- `src/pipelines/` – daily, weekly, and monthly orchestration pipelines  
- `dashboards/data/` – JSON/CSV outputs for dashboard consumption  
- `reports/` – markdown reports and diagnostics

## Running Locally

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Run a pipeline manually, for example the daily snapshot:

```bash
python -m src.orchestration.runner --pipeline daily
```

Generate the consumer quant blueprint and retail-app action plan:

```bash
python -m src.orchestration.runner --pipeline consumer
```

Produce a Snowflake benchmark analytics blueprint (schema + queries + dashboard starters):

```bash
python -m src.orchestration.runner --pipeline snowflake
```

## GitHub Integration

GitHub Actions workflows in `.github/workflows/` can be used to:

- Run daily cross-asset updates
- Generate weekly factor and risk commentary
- Produce monthly SAA reviews

Secrets for data access (e.g. Snowflake) and GitHub API access should be configured in the repository settings.

## Status

This repository provides a production-ready skeleton and reference implementation.  
Domain-specific model logic, queries, and dashboard integration points are intentionally modular in `src/tools/` and can be customized to match the actual research stack and data environment.
