# ScopeDB Demo Scene

Hands-on scenarios, sample applications, and workshop material for exploring ScopeDB.

Each demo follows a concrete question from incoming data to a useful answer. Scenes live in separate top-level directories and include their own setup, data, queries, and walkthrough.

## Demos

| Scene | What you will do | Prerequisites |
| --- | --- | --- |
| [Bluesky events](bluesky-events/README.md) | Build a checked activity briefing from a sample, then compare live Bluesky observations | ScopeDB Cloud account, Go 1.24+, Git |

## Run a demo

Open a scene's README and follow its instructions. It lists the required ScopeDB connection, tools, tested versions, and any additional services. Each walkthrough includes expected results and ways to keep exploring with your data.

Keep credentials in local environment variables or an ignored `.env` file. Committed `.env.example` files contain placeholders only.

## Repository layout

```text
README.md             Demo catalog and entry point
CONTRIBUTING.md       Guidelines for adding a scene
bluesky-events/       Public event capture and a Console walkthrough
```

Add each scene directly under the repository root using a descriptive directory name. Keep scene-specific code, queries, configuration, and assets together so the scene can be run independently.

## Add a demo

Follow the [contribution guide](CONTRIBUTING.md). Write the walkthrough for someone running the demo for the first time, with working commands and visible results at each step.

## Resources

- [ScopeDB](https://www.scopedb.io/)
- [Documentation](https://www.scopedb.io/docs)
- [ScopeDB Console](https://console.scopedb.cloud/)
