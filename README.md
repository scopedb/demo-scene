# ScopeDB demos

Produce or collect events, write them through a ScopeDB SDK or OpenTelemetry, and follow the data to a useful answer in Console.

## Choose a scene

Each scene asks one question and keeps its ScopeQL queries with the runnable integration. A dash means that implementation does not exist yet.

| Scene                                          | Question                                                            | JavaScript                    | Go                              | Rust                            | OpenTelemetry                  |
| ---------------------------------------------- | ------------------------------------------------------------------- | ----------------------------- | ------------------------------- | ------------------------------- | ------------------------------ |
| [Application logs](application-logs/README.md) | Which release and payment gateway are producing checkout errors?    | [0.3.0](application-logs/js/) | [0.7.0](application-logs/go/)   | [0.4.2](application-logs/rust/) | —                              |
| [Bluesky activity](bluesky-events/README.md)   | Are we seeing new posts, replies, or reactions to existing posts?   | —                             | [0.7.0](bluesky-events/main.go) | —                               | —                              |
| [Agent runs](agent-runs/README.md)             | What did Claude Code do between a prompt and its answer?            | —                             | —                               | —                               | [Telescope 0.3.0](agent-runs/) |

Event producers keep running until Ctrl+C; the Claude Code example follows a real task and also supports interactive sessions. Scene READMEs cover connection setup, observations, shutdown, and verification. The [ScopeDB tutorials](https://www.scopedb.io/docs/guides#follow-a-complete-example) provide illustrated Console walkthroughs.

## Start

```bash
git clone https://github.com/scopedb/demo-scene.git scopedb-demos
cd scopedb-demos
```

Choose a scene and its SDK or telemetry integration, configure the ScopeDB API address and API key for your workspace, then follow the scene README. Use a test workspace; writes, storage, and queries consume its normal allowance. Keep credentials in local environment variables and generated captures outside source control.

A scene owns its story, saved ScopeQL queries, shared data, and verification inputs. Each language directory owns its program, pinned dependencies, and tests. Programs and verification inputs live here; website tutorials refer to this checkout rather than carrying a second copy.

## Contribute

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Start with the question and intended result, use an official SDK, and make the default demo produce or ingest events continuously. Provide a bounded verification path for repeatable checks.
