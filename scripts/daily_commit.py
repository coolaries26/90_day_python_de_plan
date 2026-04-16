#!/usr/bin/env python3
"""
daily_commit.py — Daily Git Workflow Automation
================================================
Replaces all manual git commands for the daily workflow.
Handles: stage → commit → push → merge to develop → log progress.

Usage:
    python scripts/daily_commit.py \
        --day 5 \
        --sprint 1 \
        --message "Git workflow automation complete" \
        --merge          # add this flag to also merge to develop

    python scripts/daily_commit.py --day 5 --sprint 1 \
        --message "Fix: correct approach" \
        --fix            # marks this as a fix commit
"""

import argparse
import sys
from pathlib import Path
# ── PATH FIX FOR JIRA CLIENT (Day 06) ─────────────────────────────
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent / "sprint-01" / "day-06"))
print("DEBUG: Added path for jira_client →", _here.parent / "sprint-01" / "day-06") 
import os
from datetime import datetime
from pathlib import Path

from git import Repo, GitCommandError, InvalidGitRepositoryError
from jira_client import jira_client

# ── Bootstrap paths ───────────────────────────────────────────────────────────
_scripts_dir = Path(__file__).resolve().parent
_project_root = _scripts_dir.parent
sys.path.insert(0, str(_project_root / "sprint-01" / "day-04"))

#from logger import get_logger
#log = get_logger(__name__)
from logger import get_pipeline_logger
log = get_pipeline_logger("daily_commit")

# ── Progress log ──────────────────────────────────────────────────────────────
PROGRESS_LOG = _project_root / "logs" / "progress.md"


def find_repo() -> Repo:
    """Find git repo from project root."""
    try:
        return Repo(_project_root)
    except InvalidGitRepositoryError:
        log.error("No git repository found at {}", _project_root)
        sys.exit(1)


def validate_branch(repo: Repo, day: int, sprint: int) -> str:
    """
    Confirm we are on the correct feature branch for today.
    Warns if on main or develop — those should never be committed to directly.
    Returns current branch name.
    """
    try:
        branch = repo.active_branch.name
    except TypeError:
        log.error("Detached HEAD state — checkout a branch first")
        sys.exit(1)

    if branch in ("main", "develop"):
        log.warning(
            "You are on '{}' — daily work should be on a feature branch. "
            "Expected: sprint-{:02d}/day-{:02d}-*",
            branch, sprint, day
        )
        answer = input(f"  Continue committing to '{branch}'? (y/N): ").strip().lower()
        if answer != "y":
            log.info("Aborted. Create a feature branch first:")
            log.info("  git checkout -b sprint-{:02d}/day-{:02d}-your-topic", sprint, day)
            sys.exit(0)

    log.info(f"Branch | {branch}")
    return branch


def stage_all(repo: Repo) -> list[str]:
    """
    Stage all changes (git add .).
    Returns list of staged file paths.
    """
    repo.git.add(A=True)       # equivalent to git add -A (all changes including deletions)
    staged = [item.a_path for item in repo.index.diff("HEAD")]
    untracked_count = len(repo.untracked_files)
    #log.info("Staged | {} changed files, {} untracked", len(staged), untracked_count)
    log.info(f"Staged | {len(staged)} changed files, {untracked_count} untracked")
    return staged


def make_commit(repo: Repo, day: int, sprint: int,
                message: str, is_fix: bool = False) -> str:
    """
    Create a single commit with standardised message format.
    Returns the commit SHA.

    Commit format:
      [DAY-005][S01] Your message here
      [DAY-005][S01][FIX] Fix description here
    """
    # Check there is something to commit
    if not repo.is_dirty(untracked_files=True):
        log.info("Nothing to commit — working tree is clean")
        return repo.head.commit.hexsha[:8]

    prefix  = f"[DAY-{day:03d}][S{sprint:02d}]"
    prefix += "[FIX]" if is_fix else ""
    full_msg = f"{prefix} {message}"

    commit = repo.index.commit(full_msg)
    
    #log.info("Committed | {} | {}", commit.hexsha[:8], full_msg)
    log.info(f"Committed | {commit.hexsha[:8]} | {full_msg}")
    return commit.hexsha[:8]


def push_branch(repo: Repo) -> bool:
    """
    Push current branch to origin.
    Sets upstream tracking if not already set.
    Returns True on success.
    """
    branch = repo.active_branch.name
    try:
        origin = repo.remote("origin")
        # Check if upstream is already set
        tracking = repo.active_branch.tracking_branch()
        if tracking is None:
            # First push — set upstream
            origin.push(refspec=f"{branch}:{branch}", set_upstream=True)
            log.info(f"Push | upstream set → origin/{branch}")
        else:
            origin.push()
            log.info(f"Push | {branch} → origin/{branch}")
        return True
    except GitCommandError as exc:
        log.error("Push failed | {}", str(exc)[:120])
        return False


# ____________updated merge to main____________________________________
def merge_to_develop(repo: Repo, source_branch: str) -> bool:
    """
    Merge source_branch into develop and push develop.
    This makes the day's work visible on GitHub's default view.

    Returns True on success.
    """
    try:
        log.info(f"Merging {source_branch} → develop")

        repo.git.checkout("develop")
        repo.git.merge(source_branch, "--no-edit")

        origin = repo.remote("origin")
        origin.push()

        log.info("Merged and pushed develop ✅")

        # Return to original branch
        repo.git.checkout(source_branch)
        log.info(f"Returned to {source_branch}")
        return True

    except GitCommandError as exc:
        log.error("Merge failed | {}", str(exc)[:200])
        log.warning("Resolve conflicts manually, then re-run without --merge")
        try:
            repo.git.checkout(source_branch)
        except Exception:
            pass
        return False


def append_progress_log(day: int, sprint: int,
                        message: str, sha: str, branch: str) -> None:
    """Append entry to logs/progress.md."""
    PROGRESS_LOG.parent.mkdir(exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    header_needed = (
        not PROGRESS_LOG.exists() or PROGRESS_LOG.stat().st_size == 0
    )
    with open(PROGRESS_LOG, "a", encoding="utf-8") as f:
        if header_needed:
            f.write("# ��� 90-Day Progress Log\n\n")
            f.write("| Day | Sprint | Date | SHA | Branch | Notes |\n")
            f.write("|-----|--------|------|-----|--------|-------|\n")
        f.write(
            f"| {day:03d} | S{sprint:02d} | {now} "
            f"| `{sha}` | `{branch}` | {message} |\n"
        )
    log.info("Progress log updated → logs/progress.md")


# ── Q2: merge_to_main() — WRITE THIS YOURSELF ────────────────────────────────
def merge_to_main(repo: Repo) -> bool:
    """
    Q2 — YOUR TASK:
    Merge develop into main and push main.
    This is called at end of each sprint (every 7 days).

    Expected behaviour:
      1. Checkout main
      2. Pull latest main from origin (avoid conflicts)
      3. Merge develop into main with --no-edit
      4. Push main to origin
      5. Create a git tag named 'sprint-XX-complete'
         where XX comes from the current develop branch context
      6. Push the tag to origin
      7. Return to develop branch
      8. Return True on success, False on GitCommandError

    HINTS:
      - repo.git.checkout("main")
      - repo.git.pull("origin", "main")
      - repo.git.merge("develop", "--no-edit")
      - repo.git.tag("sprint-01-complete")     ← hardcode sprint for now
      - repo.remote("origin").push(tags=True)
      - repo.git.checkout("develop")

    Self-check: after running, git log --oneline -3 on main should show
    a merge commit, and git tag should list 'sprint-01-complete'.
    """
    try:
        log.info("Merging develop → main")

        # Checkout main and pull latest
        repo.git.checkout("main")
        repo.git.pull("origin", "main")

        # Merge develop into main
        repo.git.merge("develop", "--no-edit")

        # Push main
        origin = repo.remote("origin")
        origin.push()

        # Create sprint tag
        tag_name = "sprint-01-complete"
        repo.git.tag("-f", tag_name)          # -f forces overwrite if tag exists
        origin.push(tags=True)

        log.info("✅ Merged and pushed main + tag '%s'", tag_name)

        # Return to develop
        repo.git.checkout("develop")
        log.info("Returned to develop")
        return True

    except GitCommandError as exc:
        log.error("Merge to main failed | %s", str(exc)[:200])
        try:
            repo.git.checkout("develop")
        except Exception:
            pass
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Daily git commit + push + optional merge",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--day",     type=int, required=True, help="Day number (1–90)")
    parser.add_argument("--sprint",  type=int, default=None,  help="Sprint number")
    parser.add_argument("--message", "-m", required=True,     help="Commit message")
    parser.add_argument("--fix",     action="store_true",     help="Mark as a fix commit")
    parser.add_argument("--merge",   action="store_true",     help="Also merge to develop")
    parser.add_argument("--to-main", action="store_true",     help="Also merge develop to main (sprint end)")
    args = parser.parse_args()

    sprint = args.sprint or ((args.day - 1) // 7 + 1)

    log.info("=" * 52)
    log.info(f"Daily Commit | Day {args.day:03d} | Sprint {sprint:02d}"  )
    log.info("=" * 52)

    repo        = find_repo()
    branch      = validate_branch(repo, args.day, sprint)
    staged      = stage_all(repo)
    sha         = make_commit(repo, args.day, sprint, args.message, args.fix)
    pushed      = push_branch(repo)

    if args.merge and pushed:
        merge_to_develop(repo, branch)

    if args.to_main:
        try:
            merge_to_main(repo)
        except NotImplementedError as exc:
            log.warning("merge_to_main not implemented yet: {}", exc)

            # === JIRA AUTOMATION (Day 06) ===
#        try:
#            issue_key = jira_client.create_or_update_daily_task(
#                day=args.day,
#                sprint=sprint,
#                message=args.message,
#                sha=sha
#            )
#            log.info("JIRA automation complete | task={}", issue_key)
#        except Exception as exc:
#            log.error("JIRA post failed (non-blocking) | {}", exc)

    log.info("=" * 52)
    log.info(f"Day {args.day:03d} complete | SHA={sha} | pushed={pushed}")
    log.info("=" * 52)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
daily_commit.py — Daily Git Workflow Automation
================================================
Replaces all manual git commands for the daily workflow.
Handles: stage → commit → push → merge to develop → log progress.

Usage:
    python scripts/daily_commit.py \
        --day 5 \
        --sprint 1 \
        --message "Git workflow automation complete" \
        --merge          # add this flag to also merge to develop

    python scripts/daily_commit.py --day 5 --sprint 1 \
        --message "Fix: correct approach" \
        --fix            # marks this as a fix commit
"""

import argparse
import sys
import os
from datetime import datetime
from pathlib import Path

from git import Repo, GitCommandError, InvalidGitRepositoryError

# ── Bootstrap paths ───────────────────────────────────────────────────────────
_scripts_dir = Path(__file__).resolve().parent
_project_root = _scripts_dir.parent
sys.path.insert(0, str(_project_root / "sprint-01" / "day-04"))

from logger import get_logger
log = get_logger(__name__)

# ── Progress log ──────────────────────────────────────────────────────────────
PROGRESS_LOG = _project_root / "logs" / "progress.md"


def find_repo() -> Repo:
    """Find git repo from project root."""
    try:
        return Repo(_project_root)
    except InvalidGitRepositoryError:
        log.error("No git repository found at {}", _project_root)
        sys.exit(1)


def validate_branch(repo: Repo, day: int, sprint: int) -> str:
    """
    Confirm we are on the correct feature branch for today.
    Warns if on main or develop — those should never be committed to directly.
    Returns current branch name.
    """
    try:
        branch = repo.active_branch.name
    except TypeError:
        log.error("Detached HEAD state — checkout a branch first")
        sys.exit(1)

    if branch in ("main", "develop"):
        log.warning(
            "You are on '{}' — daily work should be on a feature branch. "
            "Expected: sprint-{:02d}/day-{:02d}-*",
            branch, sprint, day
        )
        answer = input(f"  Continue committing to '{branch}'? (y/N): ").strip().lower()
        if answer != "y":
            log.info("Aborted. Create a feature branch first:")
            log.info(f"  git checkout -b sprint-{sprint:02d}/day-{day:02d}-your-topic")
            sys.exit(0)

    log.info(f"Branch | {branch}")
    return branch


def stage_all(repo: Repo) -> list[str]:
    """
    Stage all changes (git add .).
    Returns list of staged file paths.
    """
    repo.git.add(A=True)       # equivalent to git add -A (all changes including deletions)
    staged = [item.a_path for item in repo.index.diff("HEAD")]
    untracked_count = len(repo.untracked_files)
    log.info(f"Staged | {len(staged)} changed files, {untracked_count} untracked")
    return staged


def make_commit(repo: Repo, day: int, sprint: int,
                message: str, is_fix: bool = False) -> str:
    """
    Create a single commit with standardised message format.
    Returns the commit SHA.

    Commit format:
      [DAY-005][S01] Your message here
      [DAY-005][S01][FIX] Fix description here
    """
    # Check there is something to commit
    if not repo.is_dirty(untracked_files=True):
        log.info("Nothing to commit — working tree is clean")
        return repo.head.commit.hexsha[:8]

    prefix  = f"[DAY-{day:03d}][S{sprint:02d}]"
    prefix += "[FIX]" if is_fix else ""
    full_msg = f"{prefix} {message}"

    commit = repo.index.commit(full_msg)
    log.info(f"Committed | {commit.hexsha[:8]} | {full_msg}")
    return commit.hexsha[:8]


def push_branch(repo: Repo) -> bool:
    """
    Push current branch to origin.
    Sets upstream tracking if not already set.
    Returns True on success.
    """
    branch = repo.active_branch.name
    try:
        origin = repo.remote("origin")
        # Check if upstream is already set
        tracking = repo.active_branch.tracking_branch()
        if tracking is None:
            # First push — set upstream
            origin.push(refspec=f"{branch}:{branch}", set_upstream=True)
            log.info(f"Push | upstream set → origin/{branch}")
        else:
            origin.push()
            log.info(f"Push | {branch} → origin/{branch}")
        return True
    except GitCommandError as exc:
        log.error("Push failed | {}", str(exc)[:120])
        return False


def merge_to_develop(repo: Repo, source_branch: str) -> bool:
    """
    Merge source_branch into develop and push develop.
    This makes the day's work visible on GitHub's default view.

    Returns True on success.
    """
    try:
        log.info(f"Merging {source_branch} → develop")

        repo.git.checkout("develop")
        repo.git.merge(source_branch, "--no-edit")

        origin = repo.remote("origin")
        origin.push()

        log.info("Merged and pushed develop ✅")

        # Return to original branch
        repo.git.checkout(source_branch)
        log.info(f"Returned to {source_branch}")
        return True

    except GitCommandError as exc:
        log.error("Merge failed | {}", str(exc)[:200])
        log.warning("Resolve conflicts manually, then re-run without --merge")
        try:
            repo.git.checkout(source_branch)
        except Exception:
            pass
        return False


def append_progress_log(day: int, sprint: int,
                        message: str, sha: str, branch: str) -> None:
    """Append entry to logs/progress.md."""
    PROGRESS_LOG.parent.mkdir(exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    header_needed = (
        not PROGRESS_LOG.exists() or PROGRESS_LOG.stat().st_size == 0
    )
    with open(PROGRESS_LOG, "a", encoding="utf-8") as f:
        if header_needed:
            f.write("# ��� 90-Day Progress Log\n\n")
            f.write("| Day | Sprint | Date | SHA | Branch | Notes |\n")
            f.write("|-----|--------|------|-----|--------|-------|\n")
        f.write(
            f"| {day:03d} | S{sprint:02d} | {now} "
            f"| `{sha}` | `{branch}` | {message} |\n"
        )
    log.info("Progress log updated → logs/progress.md")


# ── Q2: merge_to_main() — WRITE THIS YOURSELF ────────────────────────────────
def merge_to_main(repo: Repo) -> bool:
    """
    Q2 — YOUR TASK:
    Merge develop into main and push main.
    This is called at end of each sprint (every 7 days).

    Expected behaviour:
      1. Checkout main
      2. Pull latest main from origin (avoid conflicts)
      3. Merge develop into main with --no-edit
      4. Push main to origin
      5. Create a git tag named 'sprint-XX-complete'
         where XX comes from the current develop branch context
      6. Push the tag to origin
      7. Return to develop branch
      8. Return True on success, False on GitCommandError

    HINTS:
      - repo.git.checkout("main")
      - repo.git.pull("origin", "main")
      - repo.git.merge("develop", "--no-edit")
      - repo.git.tag("sprint-01-complete")     ← hardcode sprint for now
      - repo.remote("origin").push(tags=True)
      - repo.git.checkout("develop")

    Self-check: after running, git log --oneline -3 on main should show
    a merge commit, and git tag should list 'sprint-01-complete'.
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement merge_to_main — see hints above")


def main():
    parser = argparse.ArgumentParser(
        description="Daily git commit + push + optional merge",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--day",     type=int, required=True, help="Day number (1–90)")
    parser.add_argument("--sprint",  type=int, default=None,  help="Sprint number")
    parser.add_argument("--message", "-m", required=True,     help="Commit message")
    parser.add_argument("--fix",     action="store_true",     help="Mark as a fix commit")
    parser.add_argument("--merge",   action="store_true",     help="Also merge to develop")
    parser.add_argument("--to-main", action="store_true",     help="Also merge develop to main (sprint end)")
    args = parser.parse_args()

    sprint = args.sprint or ((args.day - 1) // 7 + 1)

    log.info("=" * 52)
    log.info(f"Daily Commit | Day {args.day:03d} | Sprint {sprint:02d}")
    log.info("=" * 52)

    repo        = find_repo()
    branch      = validate_branch(repo, args.day, sprint)
    staged      = stage_all(repo)
    sha         = make_commit(repo, args.day, sprint, args.message, args.fix)
    pushed      = push_branch(repo)

    if args.merge and pushed:
        merge_to_develop(repo, branch)

    if args.to_main:
        try:
            merge_to_main(repo)
        except NotImplementedError as exc:
            log.warning("merge_to_main not implemented yet: {}", exc)

    append_progress_log(args.day, sprint, args.message, sha, branch)

    log.info("=" * 52)
    log.info(f"Day {args.day:03d} complete | SHA={sha} | pushed={pushed}")
    log.info("=" * 52)


if __name__ == "__main__":
    main()

