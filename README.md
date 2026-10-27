# ScopeDB demos

Continuously produce or collect events, write them with an official ScopeDB SDK, and follow the arriving data to a useful answer in Console.

## Choose a demo

| Demo                                       | Question                                                         | Client and runtime                     |
| ------------------------------------------ | ---------------------------------------------------------------- | -------------------------------------- |
| [Application logs](application-logs/README.md) | Which release and payment gateway are producing checkout errors? | JavaScript SDK 0.3.0, Node.js 20 or later |
| [Bluesky activity](bluesky-events/README.md) | Are we seeing new posts, replies, or reactions to existing posts? | Go SDK 0.7.0, Go 1.26 or later          |

Both programs keep running until Ctrl+C. Their READMEs cover connection setup, expected observations, shutdown, and verification. The [ScopeDB tutorials](https://www.scopedb.io/docs/guides#follow-a-complete-example) provide illustrated Console walkthroughs.

## Start

```bash
git clone https://github.com/scopedb/demo-scene.git scopedb-demos
cd scopedb-demos
```

Choose a project, configure the ScopeDB API address and API key for your workspace, then follow its README. Use a test workspace; writes, storage, and queries consume its normal allowance. Keep credentials in local environment variables and generated captures outside source control.

Each project owns its program, pinned dependencies, tests, and saved ScopeQL queries. Programs and verification inputs live here; website tutorials refer to this checkout rather than carrying a second copy.

## Contribute

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Start with the question and intended result, use the appropriate official SDK, and make the default demo produce or ingest events continuously. Provide a bounded verification path for repeatable checks.
