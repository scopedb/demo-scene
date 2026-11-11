# ScopeDB demos

Runnable examples, sample applications, and integrations that show how to ingest data into ScopeDB and answer practical questions with ScopeQL.

## Demos

| Scene                                          | Question                                                          | Stack                                                                                               |
| ---------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| [Application logs](application-logs/README.md) | Which release and payment gateway are producing checkout errors?  | [JavaScript](application-logs/js/), [Go](application-logs/go/), [Rust](application-logs/rust/) SDKs |
| [Bluesky activity](bluesky-events/README.md)   | Are we seeing new posts, replies, or reactions to existing posts? | [Go](bluesky-events/) SDK + Bluesky Jetstream                                                       |

See the [ScopeDB guides](https://www.scopedb.io/docs/guides#follow-a-complete-example) for illustrated walkthroughs in Console.

## Run a demo

```bash
git clone https://github.com/scopedb/demo-scene.git scopedb-demos
cd scopedb-demos
```

Open a scene's `README.md` for prerequisites, ScopeDB workspace connection setup, run commands, and saved ScopeQL queries. Use a test workspace and keep credentials in local environment variables rather than source control.

## Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) to add a new demo.
