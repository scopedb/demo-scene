# <Scene Title>

This is an authoring template. Replace the prompts with a complete walkthrough before adding the scene to the catalog.

## What you will demonstrate

Describe the audience's question, the incoming data, and the answer they will reach. Name the ScopeDB capabilities that make the investigation possible.

- Setup time: <estimate>
- Demo duration: <estimate>
- Last verified: <date, ScopeDB version or deployment, and dependency versions>

## Data flow

Describe how data moves from its source into ScopeDB and how the audience inspects the results. Include a diagram if it helps explain the scene.

Explain the dataset shape, important identifiers, and any deliberately injected changes or failures. State whether the data is synthetic or comes from an external source.

## Prerequisites

List the ScopeDB connection requirements, permissions, tool versions, additional services, and expected resource usage. Identify any paid resources before the setup steps.

## Set up

Provide commands to configure the connection, create scene-specific resources, and start any supporting services. Explain each configuration variable and its default. Provide `.env.example` when the scene uses an environment file.

## Load data

Provide commands to load or generate the data, including a small default workload. Show how to confirm ingestion is complete and what a successful result looks like.

## Walkthrough

Build a short sequence of questions. For each step, provide the exact command or query, the expected output or invariant, and what the result means. Explain any interactive action that changes the data or application behavior.

## Verify the result

State the concrete observations that demonstrate success, including the expected answer to the opening question. Identify which values may vary between runs.

## Reset and clean up

Provide commands to stop generators and services, remove the scene's database objects and temporary files, and release any provisioned resources. State exactly what is removed and how to run the scene again.

## Troubleshooting

Document likely failures and the checks that distinguish them, such as connection errors, incomplete ingestion, mismatched time ranges, or missing dependencies.

## Further exploration

Suggest a few variations that build on the working scene. Link to relevant ScopeDB documentation and include any required dataset attribution.
