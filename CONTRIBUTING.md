# Contributing a Demo

Start with the question the audience should be able to answer. Build a short walkthrough that makes the relevant ScopeDB behavior observable and repeatable.

## Organize the scene

Create a top-level directory with a descriptive, lowercase, hyphenated name. Write its README as a tutorial that takes a user from prerequisites to useful results and further exploration.

Keep the scene self-contained. Use the files and directories it needs, for example:

```text
<scene-name>/
  README.md          Story, setup, walkthrough, expected results, and next steps
  .env.example       Connection and configuration placeholders, if needed
  compose.yaml       Supporting services, if needed
  queries/           ScopeQL setup and investigation statements
  data/              Small, reusable sample inputs
  assets/            Diagrams, screenshots, and dashboard definitions
  js/                JavaScript implementation
  go/                Go implementation
  rust/              Rust implementation
```

Keep the scene story, queries, and shared inputs at the scene root. Put each implementation in a language-named directory once the scene supports more than one SDK; a scene with a single implementation may keep it at the root until a second language is added. Every implementation must use the same table definition, run labels, and observable result so the shared queries work unchanged.

Choose an official ScopeDB SDK. Make event production or ingestion continuous by default, with Ctrl+C shutdown that settles buffered writes. Keep finite modes explicit for verification. Add shared utilities when multiple working demos need the same behavior.

## Make the walkthrough reproducible

- State the audience's question, the ScopeDB capability being demonstrated, and the expected outcome.
- List prerequisites, tested versions, setup time, demo duration, and any paid services or resources needed.
- Provide a small deterministic dataset, a generator with a fixed seed, or a bounded public-source capture that users can replay locally. Document the source, applicable usage guidance, and any limits on reproducibility.
- Show how to configure the connection, load data, confirm ingestion is complete, run the investigation, and recognize the expected result.
- Keep the user's data and workspace available for further exploration. Explain how to add another dataset or repeat an import without losing earlier work.
- Keep credentials, generated datasets, logs, and local state out of commits. Use placeholders in example configuration and a temporary directory for disposable output.
- Pin dependencies and images where applicable, and record the environment and date of the last successful run.

## Check and register the demo

Run the documented setup and walkthrough from a fresh checkout. Verify the observable result, then confirm that the user can continue with another sample or repeat an import while keeping earlier data. Run the same bounded verification for every implementation in a scene and confirm they produce equivalent observations. Include automated checks when they protect a meaningful behavior or boundary.

For performance demonstrations, record the dataset size, query, index state, hardware or compute configuration, cache conditions, and measurement method. Report measured results with that context.

Once the scene runs as documented, add it to the root catalog with a relative link, a short description, and its prerequisites. Include screenshots or recordings when they help someone understand or present the scene.
