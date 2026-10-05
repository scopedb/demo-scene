# ScopeDB Demo Scene

Hands-on scenarios, sample applications, and workshop material for exploring ScopeDB.

Each demo follows a concrete question from incoming data to a useful answer. Scenes live in separate top-level directories and include their own setup, data, queries, and walkthrough.

## Demos

The first demos are being planned. This catalog will grow as runnable scenes are added.

Each catalog entry should link to the scene and describe the question it answers, the ScopeDB capabilities it demonstrates, and its prerequisites. Group entries by use case when there are enough scenes to make navigation easier.

## Run a demo

Open a scene's README and follow its instructions. It lists the required ScopeDB connection, tools, tested versions, and any additional services. Each walkthrough includes expected results and cleanup steps.

Keep credentials in local environment variables or an ignored `.env` file. Committed `.env.example` files contain placeholders only.

## Repository layout

```text
README.md             Demo catalog and entry point
CONTRIBUTING.md       Guidelines for adding a scene
_template/README.md   Walkthrough template for new scenes
```

Add each scene directly under the repository root using a descriptive directory name. Keep scene-specific code, queries, configuration, and assets together so the scene can be run independently.

## Add a demo

Start with the [scene template](_template/README.md) and follow the [contribution guide](CONTRIBUTING.md).

## Resources

- [ScopeDB](https://www.scopedb.io/)
- [Documentation](https://docs.scopedb.io/)
- [ScopeDB Console](https://console.scopedb.cloud/)
