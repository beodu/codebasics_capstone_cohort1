"""Command line for the AtliQ warehouse."""
from __future__ import annotations

import argparse
import sys

from . import bootstrap, dashboard, nightly, quality, serve as serve_mod, simulate, streaming
from .loggingutil import log


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="atliq",
        description="AtliQ Commerce warehouse on local SQL Server",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("setup", help="create databases, load OLTP seed, create warehouse schemas")

    run_p = sub.add_parser("run", help="nightly ingest → silver MERGE → gold")
    run_p.add_argument("--twice", action="store_true", help="run twice and assert idempotency")
    run_p.add_argument("--simulate", type=int, metavar="N", help="insert N new orders first")
    run_p.add_argument("--quiet", action="store_true")

    sim_p = sub.add_parser("simulate", help="insert new/changed rows into OLTP")
    sim_p.add_argument("--orders", type=int, default=8)
    sim_p.add_argument("--updates", type=int, default=5)

    sub.add_parser("quality", help="run the 13 data-quality checks")
    sub.add_parser("dashboard", help="write a snapshot HTML file from gold")

    srv = sub.add_parser("serve", help="live dashboard (re-queries SQL Server)")
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=8080)

    st = sub.add_parser("stream", help="produce and process the order-event stream")
    st.add_argument("--events", type=int, default=400)
    st.add_argument("--replay", type=int, default=25, help="percent of events to duplicate")
    st.add_argument("--reset", action="store_true")
    st.add_argument("--dq-only", action="store_true")
    st.add_argument("--max-age", type=int, default=120)

    args = p.parse_args(argv)

    if args.cmd == "setup":
        bootstrap.setup()
        return 0

    if args.cmd == "run":
        verbose = not args.quiet
        if args.simulate:
            simulate.run(n_orders=args.simulate, verbose=verbose)
        if args.twice:
            ok = nightly.run_twice(verbose=verbose)
            return 0 if ok else 1
        _, snap = nightly.run_once(verbose=verbose)
        quality.run(verbose=verbose)
        nightly.print_snapshot(snap)
        dashboard.write_snapshot()
        return 0

    if args.cmd == "simulate":
        simulate.run(n_orders=args.orders, n_updates=args.updates)
        return 0

    if args.cmd == "quality":
        results = quality.run()
        return 0 if all(ok for _, _, ok in results) else 1

    if args.cmd == "dashboard":
        path = dashboard.write_snapshot()
        log(f"wrote {path}")
        return 0

    if args.cmd == "serve":
        serve_mod.serve(args.host, args.port)
        return 0

    if args.cmd == "stream":
        if args.dq_only:
            from .sqlclient import Sql
            from . import config as cfg
            with Sql(cfg.DW_DB) as dw:
                ok, msg = streaming.dq_fresh(dw, args.max_age)
                log(("PASS" if ok else "FAIL") + " — " + msg)
                return 0 if ok else 1
        streaming.run(events=args.events, replay=args.replay, reset=args.reset)
        return 0

    p.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
