# ScopeDB demos

Continuously produce or collect events, write them with an official ScopeDB SDK, and follow the arriving data to a useful answer in Console.

## Choose a scene

Each scene asks one question and shares its ScopeQL queries and expected results across language implementations. A dash means that implementation does not exist yet.

| Scene                                          | Question                                                            | JavaScript                    | Go                              | Rust                            |
| ---------------------------------------------- | ------------------------------------------------------------------- | ----------------------------- | ------------------------------- | ------------------------------- |
| [Application logs](application-logs/README.md) | Which release and payment gateway are producing checkout errors?    | [0.3.0](application-logs/js/) | [0.7.0](application-logs/go/)   | [0.4.2](application-logs/rust/) |
| [Bluesky activity](bluesky-events/README.md)   | Are we seeing new posts, replies, or reactions to existing posts?   | —                             | [0.7.0](bluesky-events/main.go) | —                               |
| [Agent runs](agent-runs/README.md)             | Is a slow agent run waiting on a model or a tool timeout and retry? | —                             | —                               | [0.4.2](agent-runs/src/main.rs) |

Every program keeps running until Ctrl+C. Scene READMEs cover connection setup, expected observations, shutdown, and verification. The [ScopeDB tutorials](https://www.scopedb.io/docs/guides#follow-a-complete-example) provide illustrated Console walkthroughs.

## Start

```bash
git clone https://github.com/scopedb/demo-scene.git scopedb-demos
cd scopedb-demos
```

Choose a scene and one of its SDKs, configure the ScopeDB API address and API key for your workspace, then follow the scene README. Use a test workspace; writes, storage, and queries consume its normal allowance. Keep credentials in local environment variables and generated captures outside source control.

A scene owns its story, saved ScopeQL queries, shared data, and verification inputs. Each language directory owns its program, pinned dependencies, and tests. Programs and verification inputs live here; website tutorials refer to this checkout rather than carrying a second copy.

## Contribute

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Start with the question and intended result, use an official SDK, and make the default demo produce or ingest events continuously. Provide a bounded verification path for repeatable checks.
