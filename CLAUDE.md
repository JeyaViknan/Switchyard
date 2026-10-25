# CLAUDE.md — Switchyard

Switchyard is a multi-tenant LLM gateway (token-fair scheduling, hard budgets, circuit breaking, phased failover).
The product code is finished and tested. Current work: **make it a deployed, monitored, $0 product with a live web console.**

**Read `docs/HANDOVER-DEVOPS.md` before any work.** It is the source of truth for scope, milestones and acceptance criteria.
Also read `docs/decisions.md`: it explains why the system is built the way it is, and new decisions are recorded there in the same style.

## Who you're working with
Jeya is preparing for a DevOps intern interview on **Tue 27 Oct 2026** and must be able to explain every line of this work.
- After each milestone, write `docs/learning/M<n>.md` (≤ 30 lines): what was built, why it was designed that way, and 3–5 likely interview questions with short answers.
- Explain non-obvious choices in commit messages and as new entries in `docs/decisions.md`.
- Prefer the simplest thing that works. If two designs are equal, pick the one that is easier to explain out loud.

## Commands
```
make install      # venv + dev deps
make check        # ruff + mypy (strict on core/adapters) + 356 tests -- must stay green
make test-all     # + slow end-to-end scenario tests
make up / down    # local Compose stack (gateway, fleet, console, Prometheus, Grafana)
make prod-local   # production Compose file run locally with dev secrets (added in M4)
make demo         # CLI scenarios
```

## Hard rules
1. **$0, always.** Only these AWS services: one EC2 `t3.micro` (credit specification **standard**, never unlimited), its 30 GB gp3 root volume, default VPC + one security group, IAM (instance role, GitHub OIDC provider + deploy role), SSM Parameter Store (Standard tier, AWS-managed key), SSM Run Command + Session Manager, one CloudWatch alarm, one AWS Budgets zero-spend budget.
   Never add: NAT gateway, load balancers, ECS/EKS/Fargate/App Runner, RDS/ElastiCache/EFS/DynamoDB, CloudFront, Route 53 zones, Elastic IPs, Secrets Manager, customer-managed KMS keys, CloudWatch Logs agents, detailed monitoring, snapshots, a second instance, or any other instance type. If something seems to need a paid service, stop and ask.
2. **No new tools.** Allowed: Python/FastAPI (already used), plain HTML/CSS/JavaScript (no Node toolchain, no frontend framework, no build step), Docker + Compose, Terraform, Prometheus, Grafana, GitHub Actions, GHCR, AWS CLI, the Trivy action. No Kubernetes, Redis, nginx/Caddy, Alertmanager, Loki, Helm, Ansible.
3. **Never run `terraform apply`, `terraform destroy`, or any AWS CLI command that creates, changes or deletes resources** unless Jeya says so in this session. `terraform fmt/validate/plan` and read-only `describe`/`get` calls are fine.
4. **No secrets in git, images, logs, Terraform state or the browser.** Key *hashes* in `deploy/switchyard.prod.toml` are fine (useless without the pepper). Raw keys, the pepper and passwords live only in SSM Parameter Store and in root-only env files on the instance.
5. **Work on a branch** (`devops`), small conventional commits (`feat(console): ...`, `ci: ...`, `infra: ...`, `docs: ...`). Push or open a PR only when Jeya asks: a merge to `main` deploys.
6. `make check` green before every commit. New behaviour gets tests. Do not change scheduling, budget or failover semantics; the console and deployment sit *around* the product.
7. **Honesty in the product.** The UI must say when it is running on synthetic providers. Never write "production", "real users" or numbers you didn't measure.
8. Stop at every ⛔ checkpoint in the handover and tell Jeya exactly what to run and what to expect.

## Code style
- Match the existing code: type hints everywhere, small modules, docstrings that explain *why*, invariants enforced in code rather than by convention.
- New Python lives under `src/switchyard/` and ships in the same image. mypy strict applies to new `core` code; keep the console typed too.
- Frontend: semantic HTML, CSS variables for light/dark, vanilla ES modules, SVG for the live map. Status is never shown by colour alone (icon + text). Respect `prefers-reduced-motion`.
- Times stored in UTC ISO-8601, shown in the viewer's local time.
