# Contributing a Demo

Start with the question the audience should be able to answer. Build a short walkthrough that makes the relevant ScopeDB behavior observable and repeatable.

## Organize the scene

Create a top-level directory with a descriptive, lowercase, hyphenated name. Copy `_template/README.md` into it and replace the prompts with the actual walkthrough.

Keep the scene self-contained. Use the files and directories it needs, for example:

```text
<scene-name>/
  README.md          Story, setup, walkthrough, expected results, and cleanup
  .env.example       Connection and configuration placeholders, if needed
  compose.yaml       Supporting services, if needed
  app/               Sample application or data generator
  queries/           ScopeQL setup, investigation, and cleanup statements
  data/              Small, reusable sample inputs
  assets/            Diagrams, screenshots, and dashboard definitions
```

Choose the language and tools that fit the scene. Add shared utilities when multiple working demos need the same behavior.

## Make the walkthrough reproducible

- State the audience's question, the ScopeDB capability being demonstrated, and the expected outcome.
- List prerequisites, tested versions, setup time, demo duration, and any paid services or resources needed.
- Provide a small deterministic dataset or a generator with a fixed seed and a bounded default workload. Document the source and license of third-party data.
- Show how to configure the connection, load data, confirm ingestion is complete, run the investigation, and recognize the expected result.
- Explain how to reset and repeat the scene. Document the exact resources cleanup removes, and scope cleanup to resources created by that scene.
- Keep credentials, generated datasets, logs, and local state out of commits. Use placeholders in example configuration and a temporary directory for disposable output.
- Pin dependencies and images where applicable, and record the environment and date of the last successful run.

## Check and register the demo

Run the documented setup, walkthrough, and cleanup from a fresh checkout. Verify the observable result and repeat the demo after cleanup. Include automated checks when they protect a meaningful behavior or boundary.

For performance demonstrations, record the dataset size, query, index state, hardware or compute configuration, cache conditions, and measurement method. Report measured results with that context.

Once the scene runs as documented, add it to the root catalog with a relative link, a short description, and its prerequisites. Include screenshots or recordings when they help someone understand or present the scene.
