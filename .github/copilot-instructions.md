# Project instructions

## Context

This repository implements the NsTech Data Platform technical challenge in
`case-cdc-vehicle-cache.md`. That file is the official requirements source.
The solution targets .NET 10 and uses PostgreSQL, Debezium, Kafka, and Valkey.

## Engineering principles

- Keep the solution simple, functional, reliable, and maintainable.
- Apply Clean Code, KISS, YAGNI, and SOLID pragmatically.
- Use descriptive names, focused methods, strongly typed C#, and explicit behavior.
- Write code and technical documentation in English. Provide execution summaries in Portuguese.
- Do not add unnecessary layers, abstractions, dependencies, services, or frameworks.
- Do not add MediatR, CQRS frameworks, or generic repositories.

## Architecture constraints

- The Worker must derive its projection only from CDC and must never access PostgreSQL.
- Mock writers write exclusively to PostgreSQL; they do not write to Kafka or Valkey.
- The cache is derived entirely from CDC and must support deterministic replay.
- Preserve ordering, idempotency, and atomic conditional Valkey updates.
- Reason explicitly about independent topic order, duplicates, snapshot/stream transitions,
  position-before-vehicle delivery, deletes, tombstones, restart, and offset commits.
- Distinguish database log order from business-event order. Do not treat an LSN alone as
  a unique event identifier.

## Workflow

- Follow RFC -> ADR -> Spec -> Tests -> Implementation -> Pull Request.
- Read the official challenge and relevant architecture documents before implementation.
- Work on one phase at a time; do not start a later phase without approval.
- Keep architectural decisions in Proposed status until human review.
- Explain any change to an approved decision and keep documentation synchronized with code.
- Never merge into `main` or push changes without explicit approval.

## Quality and validation

- Prioritize correctness over feature quantity; test critical business rules and failure cases.
- Run relevant build and test commands after implementation and report only verified results.
- Do not invent test results, claim unexecuted commands succeeded, or hide blockers.
- Do not change the target framework if the required SDK is missing.
- Report assumptions, unresolved questions, and recommended next steps.
