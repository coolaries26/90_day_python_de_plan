#!/usr/bin/env python3
"""
verify_setup.py — Day 01 | Sprint 01
Full stack environment verification:
  - Python version
  - Required packages
  - PostgreSQL connection via psycopg2
  - PostgreSQL connection via SQLAlchemy
  - appuser privilege check (ensure NOT superuser)
  - Memory/connection leak check (pool cleanup)
  - Git status

Run: python verify_setup.py
"""

import sys
import os
import importlib
import subprocess
from pathlib import Path

# ── Resolve .env path explicitly ─────────────────────────────────────────────
# load_dotenv() with no args only searches CWD — unreliable when running from
# a subdirectory.  We walk UP from this script until we find .env or hit root.

def _find_and_load_dotenv() -> Path | None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return None                          # flagged in package check below

    # 1. Check alongside this script
    script_dir = Path(__file__).resolve().parent
    for candidate in [
        script_dir / ".env",                 # same folder as verify_setup.py
        script_dir.parent / ".env",          # sprint-01/ level
        script_dir.parent.parent / ".env",   # project root  ← most likely
        Path.home() / "python-de-journey" / ".env",  # absolute fallback
    ]:
        if candidate.is_file():
            load_dotenv(dotenv_path=candidate, override=False)
            return candidate

    # 2. Also try CWD as last resort
    cwd_env = Path.cwd() / ".env"
    if cwd_env.is_file():
        load_dotenv(dotenv_path=cwd_env, override=False)
        return cwd_env

    return None   # .env not found anywhere

_env_path = _find_and_load_dotenv()

# ─────────────────────────────────────────────────────────
REQUIRED_PACKAGES = {
    "psycopg2":   "psycopg2-binary",
    "sqlalchemy": "SQLAlchemy",
    "pandas":     "pandas",
    "numpy":      "numpy",
    "dotenv":     "python-dotenv",
    "loguru":     "loguru",
    "yaml":       "pyyaml",
    "click":      "click",
}

PASS = "  ✅"
FAIL = "  ❌"
WARN = "  ⚠️ "

results: dict[str, bool] = {}


# ─────────────────────────────────────────────────────────
def section(title: str) -> None:
    print(f"\n{'─' * 54}")
    print(f"  {title}")
    print(f"{'─' * 54}")


def check(label: str, passed: bool, detail: str = "") -> None:
    icon = PASS if passed else FAIL
    line = f"{icon} {label}"
    if detail:
        line += f"  →  {detail}"
    print(line)
    results[label] = passed


# ─────────────────────────────────────────────────────────
def check_python_version() -> None:
    section("1 · Python Version")
    v = sys.version_info
    ok = v.major == 3 and v.minor >= 11
    check(
        f"Python {v.major}.{v.minor}.{v.micro}",
        ok,
        "" if ok else "Upgrade to Python 3.11+",
    )
    check("64-bit interpreter", sys.maxsize > 2**32)


def check_packages() -> None:
    section("2 · Required Packages")
    for module, pip_name in REQUIRED_PACKAGES.items():
        try:
            mod = importlib.import_module(module)
            version = getattr(mod, "__version__", "n/a")
            check(f"{pip_name}", True, version)
        except ImportError:
            check(f"{pip_name}", False, f"pip install {pip_name}")


def check_env_vars() -> None:
    section("3 · Environment Variables (.env)")

    # Show exactly where .env was loaded from (or wasn't)
    if _env_path:
        print(f"  📂 .env loaded from: {_env_path}")
    else:
        print(f"  {FAIL} .env file NOT FOUND")
        print(f"       Searched:")
        script_dir = Path(__file__).resolve().parent
        for p in [
            script_dir / ".env",
            script_dir.parent / ".env",
            script_dir.parent.parent / ".env",
            Path.home() / "python-de-journey" / ".env",
            Path.cwd() / ".env",
        ]:
            print(f"         • {p}  {'✅ exists' if p.is_file() else '❌ missing'}")
        print()
        print(f"       FIX: Create .env at your project root:")
        print(f"         ~/python-de-journey/.env")
        print(f"       Or re-run setup_postgresql.sh to regenerate it.")

    required_vars = {
        "DB_HOST":     "127.0.0.1",
        "DB_PORT":     "5432",
        "DB_NAME":     "dvdrental",
        "DB_USER":     "appuser",
        "DB_PASSWORD": None,          # no safe default — must be explicit
    }
    print()
    all_set = True
    for var, default in required_vars.items():
        val = os.getenv(var, "")
        if not val and default:
            # Apply safe default and warn
            os.environ[var] = default
            val = default
            display = f"{val}  (using default — set in .env to be explicit)"
        elif var == "DB_PASSWORD":
            display = "***" if val else "(not set — required)"
        else:
            display = val or "(not set)"
        ok = bool(val)
        all_set = all_set and ok
        check(f"${var}", ok, display)

    if not all_set:
        print(f"\n  ⚠️  Missing vars — add them to your .env file:")
        print( "     DB_HOST=127.0.0.1")
        print( "     DB_PORT=5432")
        print( "     DB_NAME=dvdrental")
        print( "     DB_USER=appuser")
        print( "     DB_PASSWORD=AppUser@2024!")
        print( "     (change password to match what setup_postgresql.sh used)")


def check_psycopg2_connection() -> None:
    section("4 · PostgreSQL Connection (psycopg2 + Connection Pool)")

    pool = None
    try:
        import psycopg2
        from psycopg2 import pool as pg_pool

        db_cfg = {
            "host":            os.getenv("DB_HOST", "127.0.0.1"),
            "port":            int(os.getenv("DB_PORT", 5432)),
            "dbname":          os.getenv("DB_NAME", "dvdrental"),
            "user":            os.getenv("DB_USER", "appuser"),
            "password":        os.getenv("DB_PASSWORD"),
            "connect_timeout": 5,
        }

        # Use minimal pool — guarantees cleanup in finally
        pool = pg_pool.SimpleConnectionPool(minconn=1, maxconn=2, **db_cfg)
        conn = pool.getconn()

        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            pg_ver: str = cur.fetchone()[0]

            cur.execute("SELECT current_user, current_database();")
            cur_user, cur_db = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';")
            table_count: int = cur.fetchone()[0]

            # Check appuser is NOT superuser (security assertion)
            cur.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname = %s;",
                (cur_user,),
            )
            row = cur.fetchone()
            is_super = row[0] if row else None

        pool.putconn(conn)  # return to pool — critical to prevent leak

        check("psycopg2 connection",    True,  f"user={cur_user} db={cur_db}")
        check("PostgreSQL reachable",   True,  pg_ver[:50])
        check("Public tables visible",  table_count > 0, f"{table_count} tables")
        check(
            "appuser is NOT superuser",
            is_super is False,
            "SECURITY PASS" if is_super is False else "⚠️  appuser has SUPERUSER — FIX THIS",
        )

    except ImportError:
        check("psycopg2 import", False, "pip install psycopg2-binary")
    except Exception as exc:
        check("psycopg2 connection", False, str(exc)[:80])
    finally:
        if pool:
            pool.closeall()  # ← prevents connection leak no matter what


def check_sqlalchemy_connection() -> None:
    section("5 · SQLAlchemy Engine (ORM Layer)")

    engine = None
    try:
        from sqlalchemy import create_engine, text, inspect

        from urllib.parse import quote_plus
        # quote_plus encodes special chars (@, !, #, $) in password
        # that would otherwise break URL parsing
        _pwd = quote_plus(os.getenv('DB_PASSWORD', ''))
        db_url = (
            f"postgresql+psycopg2://"
            f"{os.getenv('DB_USER')}:{_pwd}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
            f"/{os.getenv('DB_NAME')}"
        )

        engine = create_engine(
            db_url,
            pool_size=2,
            max_overflow=1,
            pool_pre_ping=True,    # test conn before checkout
            pool_recycle=1800,     # recycle every 30 min
            echo=False,
        )

        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database(), pg_postmaster_start_time()::text;"))
            db_name, start_time = result.fetchone()

            insp = inspect(engine)
            tables = insp.get_table_names(schema="public")

        check("SQLAlchemy engine",      True,  f"db={db_name}")
        check("Introspect tables",      len(tables) > 0, f"{len(tables)} tables found")
        check("pool_pre_ping enabled",  True,  "stale-connection protection ON")
        check("pool_recycle=1800",      True,  "connection recycled every 30 min")

        # Show first 5 tables
        print(f"\n     📋 First 5 tables: {', '.join(tables[:5])}")

    except ImportError:
        check("SQLAlchemy import", False, "pip install SQLAlchemy")
    except Exception as exc:
        check("SQLAlchemy connection", False, str(exc)[:80])
    finally:
        if engine:
            engine.dispose()  # ← closes all pool connections — prevents leak


def check_dvdrental_data() -> None:
    section("6 · DVD Rental Data Integrity")

    pool = None
    try:
        from psycopg2 import pool as pg_pool

        db_cfg = {
            "host":     os.getenv("DB_HOST", "127.0.0.1"),
            "port":     int(os.getenv("DB_PORT", 5432)),
            "dbname":   os.getenv("DB_NAME", "dvdrental"),
            "user":     os.getenv("DB_USER", "appuser"),
            "password": os.getenv("DB_PASSWORD"),
        }

        pool = pg_pool.SimpleConnectionPool(minconn=1, maxconn=2, **db_cfg)
        conn = pool.getconn()

        EXPECTED_TABLES = [
            "film", "actor", "customer", "rental",
            "payment", "inventory", "store", "staff",
            "category", "language",
        ]
        with conn.cursor() as cur:
            for table in EXPECTED_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table};")  # noqa: S608
                cnt = cur.fetchone()[0]
                check(f"Table: {table}", cnt > 0, f"{cnt:,} rows")

        pool.putconn(conn)

    except Exception as exc:
        check("DVD Rental data", False, str(exc)[:80])
    finally:
        if pool:
            pool.closeall()


def check_git() -> None:
    section("7 · Git Repository")
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            capture_output=True, text=True, cwd=Path(__file__).parent,
        )
        check("Git repository initialised", result.returncode == 0)

        branch = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True, text=True,
        ).stdout.strip()
        check("Current branch", bool(branch), branch)

        remote = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True,
        )
        has_remote = remote.returncode == 0
        check("Remote origin set", has_remote,
              remote.stdout.strip() if has_remote else "Run: git remote add origin <URL>")

    except FileNotFoundError:
        check("Git installed", False, "Install git first")


# ─────────────────────────────────────────────────────────
def main() -> None:
    print("\n" + "=" * 54)
    print("  DAY 01 — Full Stack Environment Verification")
    print("  python-de-journey | Sprint 01")
    print("=" * 54)

    check_python_version()
    check_packages()
    check_env_vars()
    check_psycopg2_connection()
    check_sqlalchemy_connection()
    check_dvdrental_data()
    check_git()

    # ── Summary ──────────────────────────────────────────
    print(f"\n{'=' * 54}")
    total   = len(results)
    passed  = sum(1 for v in results.values() if v)
    failed  = total - passed
    pct     = int(passed / total * 100) if total else 0

    print(f"  RESULT:  {passed}/{total} checks passed  ({pct}%)")
    if failed:
        print(f"\n  ❌ Failed checks — fix before Day 02:")
        for label, ok in results.items():
            if not ok:
                print(f"     • {label}")
    else:
        print("\n  🎉 All checks passed — environment is ready!")
    print("=" * 54 + "\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
