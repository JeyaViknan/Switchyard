"""The `switchyard` command.

argparse rather than a CLI framework: the command surface is small and stable,
and a dependency-free entry point keeps the install story to one line.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Sequence

DEFAULT_URL = "http://127.0.0.1:8000"
ADMIN_KEY_ENV = "SWITCHYARD_ADMIN_KEY"


def _admin_key(args: argparse.Namespace) -> str | None:
    return args.key or os.environ.get(ADMIN_KEY_ENV)


def cmd_top(args: argparse.Namespace) -> int:
    from switchyard.cli.top import run

    return asyncio.run(run(args.url.rstrip("/"), _admin_key(args), args.interval, args.once))


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from switchyard.core.config import ConfigError, load_config

    try:
        load_config(args.config)          # fail fast with a readable message
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    os.environ.setdefault("SWITCHYARD_CONFIG", args.config)
    uvicorn.run(
        "switchyard.gateway.app:create_app", factory=True,
        host=args.host, port=args.port, log_level=args.log_level,
    )
    return 0


def cmd_scenario(args: argparse.Namespace) -> int:
    from switchyard.scenarios import SCENARIOS, names
    from switchyard.scenarios.base import Reporter

    module = SCENARIOS.get(args.name)
    if module is None:
        print(f"unknown scenario {args.name!r}. Available: {', '.join(names())}",
              file=sys.stderr)
        return 2

    reporter = Reporter()
    options = {"policy": args.policy} if args.policy else {}
    if args.seed is not None:
        options["seed"] = args.seed
    result = asyncio.run(module.run(reporter, **options))
    reporter.verdict(result)
    return result.exit_code


def cmd_keys_mint(args: argparse.Namespace) -> int:
    from switchyard.core.auth import PEPPER_ENV, mint_admin_key, mint_key

    if args.admin:
        raw, digest = mint_admin_key()
        field, section = "admin_key_sha256", "[gateway]"
    else:
        if not args.tenant_id:
            print("give a tenant id, or --admin for an operational key", file=sys.stderr)
            return 2
        raw, digest = mint_key(args.tenant_id)
        field, section = "key_sha256", "[[tenants]]"

    print(f"key    {raw}")
    print(f"digest {digest}\n")
    print(f"Add to your config under {section}:")
    if not args.admin:
        print(f'id = "{args.tenant_id}"')
    print(f'{field} = "{digest}"')
    print("\nThe key is shown once; only the digest is stored.")
    if PEPPER_ENV not in os.environ:
        print(f"Note: {PEPPER_ENV} is unset, so the development pepper was used.")
    return 0


def cmd_config_check(args: argparse.Namespace) -> int:
    from switchyard.core.config import ConfigError, load_config

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"invalid: {exc}", file=sys.stderr)
        return 1

    reserved = sum(t.reserved_concurrency for t in config.tenants)
    print(f"{args.config} is valid.\n")
    print(f"  capacity          {config.max_concurrency} concurrent requests")
    print(f"  policy            {config.scheduling_policy}")
    print(f"  reserved floors   {reserved} ({config.max_concurrency - reserved} shared)")
    print(f"  tenants           {len(config.tenants)}")
    for t in config.tenants:
        budget = f"{t.budget_tokens:,} tokens" if t.budget_tokens else "unlimited"
        print(f"    {t.id:<12} weight {t.weight:<5g} floor {t.reserved_concurrency:<3} "
              f"ceiling {t.max_concurrency or '-':<4} budget {budget}")
    print(f"  providers         {', '.join(config.providers)}")
    for model, route in config.routes.items():
        print(f"    {model:<12} -> {' then '.join(route)}")
    if not config.admin_key_sha256 and config.tenants:
        print("\n  warning: no gateway.admin_key_sha256 set, so the operational")
        print("           endpoints are disabled. Mint one with:")
        print("             switchyard keys mint --admin")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="switchyard",
        description="Multi-tenant LLM gateway: fair scheduling, budgets, failover.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the gateway")
    serve.add_argument("--config", default="switchyard.toml")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(func=cmd_serve)

    top = sub.add_parser("top", help="live view of queues, capacity and provider health")
    top.add_argument("--url", default=DEFAULT_URL)
    top.add_argument("--key", default=None, help=f"admin key (or ${ADMIN_KEY_ENV})")
    top.add_argument("--interval", type=float, default=1.0)
    top.add_argument("--once", action="store_true", help="print one snapshot and exit")
    top.set_defaults(func=cmd_top)

    from switchyard.scenarios import names as scenario_names

    scenario = sub.add_parser(
        "scenario", help="run a product scenario against the synthetic provider"
    )
    scenario.add_argument("name", choices=scenario_names(), metavar="NAME",
                          help="one of: " + ", ".join(scenario_names()))
    scenario.add_argument("--policy", choices=["drr", "fifo"], default=None,
                          help="scheduling policy to demonstrate")
    scenario.add_argument("--seed", type=int, default=None)
    scenario.set_defaults(func=cmd_scenario)

    keys = sub.add_parser("keys", help="mint credentials")
    keys_sub = keys.add_subparsers(dest="keys_command", required=True)
    mint = keys_sub.add_parser("mint", help="mint a tenant or admin key")
    mint.add_argument("tenant_id", nargs="?")
    mint.add_argument("--admin", action="store_true", help="mint an operational key")
    mint.set_defaults(func=cmd_keys_mint)

    check = sub.add_parser("check", help="validate and summarise a configuration")
    check.add_argument("--config", default="switchyard.toml")
    check.set_defaults(func=cmd_config_check)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
