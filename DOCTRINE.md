# Doctrine: postgres (FGP Postgres Daemon)

## Purpose
- Provide fast Postgres operations for agent workflows.
- Reduce latency for common API queries and actions.

## Scope
- Postgres API integration and FGP method surface.
- Local daemon execution and request routing.

## Non-Goals
- Full replacement for official Postgres clients.
- Managing billing or unrelated admin workflows.
- Hosting or multi-tenant service operation.

## Tenets
- Keep API calls explicit and minimal.
- Favor predictable responses over breadth.
- Warm-call performance is the primary metric.
- Avoid leaking sensitive data in logs.

## Architecture
- FGP daemon handles socket requests and dispatch.
- Postgres API calls authenticated via local credentials.

## Interfaces
- FGP methods for Postgres workflows.
- CLI entrypoints via `fgp call`.

## Operational Model
- Runs locally with provider credentials.
- Owners: Postgres daemon maintainers.

## Testing
- Integration tests for core endpoints.
- Error handling for auth and rate limits.

## Security
- Credentials provided via environment or local config.
- Avoid logging sensitive payloads.

## Observability
- Include timing metadata in responses.
- Surface API errors with context.

## Risks
- API changes or rate limits affecting reliability.

## Roadmap
- Expand coverage for additional Postgres workflows.
