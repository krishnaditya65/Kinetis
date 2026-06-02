# Documentation

Project documentation, organized by concern.

| Folder | Contents |
|---|---|
| [design/](design/) | The *why*: [architecture](design/architecture.md), [state machine](design/state-machine.md), [design rationale](design/README.md) |
| [implementation/](implementation/) | The *how*: module layout, request flow, concurrency model, extension points |
| [testing/](testing/) | Test philosophy, inventory (12 tests), how to run, environment quirks |
| [adr/](adr/) | Architecture Decision Records — one decision each, with trade-offs |
| [specs/](specs/) | System spec: [API/OpenAPI](specs/openapi.yaml), data model & schema evolution, deployment/topology, dependency graph, threat model |

Top-level: [README](../README.md) · [CHANGELOG](../CHANGELOG.md)

## Reading order

1. **New here?** [design/README.md](design/README.md) → [design/architecture.md](design/architecture.md)
2. **Going to change code?** [implementation/README.md](implementation/README.md) +
   [design/state-machine.md](design/state-machine.md)
3. **Integrating / operating?** [specs/README.md](specs/README.md) + [specs/openapi.yaml](specs/openapi.yaml)
4. **Why is it built this way?** [adr/](adr/)
