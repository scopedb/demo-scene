# Contributing a Demo

Each demo lives in its own top-level directory and shows a realistic ScopeDB workflow that users can run end to end on their own.

## Design the scene

- **Start from a real scenario**: Pick a concrete problem or integration and follow the data from ingestion to useful ScopeQL queries.
- **Use the natural stack**: Build the demo the way a developer actually uses that tool or platform—whether that is a ScopeDB SDK, the HTTP API, a web framework, OpenTelemetry / Telescope, or a data pipeline connector. Do not force a web application, one-shot task, or platform integration into an artificial CLI loop or multi-language matrix.
- **Keep it self-contained**: Place code, schema or catalog definitions, saved queries, and configuration placeholders inside `<scene-name>/` (lowercase, hyphenated) using whatever layout fits the ecosystem.

## Document how to run it

Write `<scene-name>/README.md` so someone starting from a fresh checkout can run the demo without guessing:

1. What the demo does and what prerequisites it needs (runtime, ScopeDB workspace, and any optional external services).
2. How to configure connection settings, prepare tables or views, and run the application or workload.
3. Which ScopeQL queries to run and what results to look for.

Keep credentials, local `.env` files, generated logs, and build outputs out of version control; commit `.env.example` placeholders when environment variables are needed.

## Verify and open a pull request

1. Run through `<scene-name>/README.md` from a clean checkout against a ScopeDB workspace to confirm everything works as documented.
2. If the scene includes automated build, lint, or unit checks that run without live credentials, wire them into [`.github/workflows/check.yml`](.github/workflows/check.yml).
3. Add the scene to the table in [`README.md`](README.md).
