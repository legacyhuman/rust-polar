# Implementation agent brief

Implement the project described in `SPEC.md` in this directory. That document is the primary requirements contract. The task is designed for GPT-6 Luna: work in small, verifiable M0–M5 milestones and record decisions and progress in a concise `IMPLEMENTATION_STATUS.md` so that resuming does not require reconstructing the full context.

The user explicitly requests active use of subagents for independent tasks and reviews. Follow §14 of the specification: establish shared contracts, assign separate file ownership, implement in parallel, integrate, and verify independently. Do not replace subagents with new user-facing chats. Use available slots when independent tasks exist; do not delegate conflicting changes to shared files.

Start by checking workspace rules, the user's files, and technical milestone M0. Do not rewrite the specification instead of implementing it. Do not weaken AccessLink completeness, response preservation, time precision, or memory bounds for the sake of a quick prototype. Do not invent unverified values.

Use Polar AccessLink only. Do not use BLE, raw sensor signals, or Open Health Sync. The code/database must use Rust and redb; verify that TLS and compression backends meet the Rust-only requirement. The archive stores original cloud responses. For Nightscout, prepare the model/interfaces and synthetic test data; implement the real connector in the next phase.

Do not control the browser. Do not switch `gh` accounts, modify the remote repository, or push. For live OAuth, show the URL to the user and wait for authorization; complete independent implementation and tests without credentials.

Do not request confirmation for routine decisions already defined by the specification. If an external incompatibility genuinely blocks a requirement, present a concrete choice and continue the independent work. Do not claim completion based on one successful example: perform the checks in §13 and provide the final report required by §15.
