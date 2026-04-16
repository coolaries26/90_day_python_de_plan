# 📅 DAY 05 — Sprint 01 | Git Workflow Mastery
## Daily Git Discipline + GitPython Automation

---

## 🔁 RETROSPECTIVE — Day 04 (Complete BEFORE starting Day 05)

### Repo Cleanup First — Run This Once
This fixes the tangled history from Days 01–04 before we build clean habits going forward:

```bash
cd C:\Users\Lenovo\python-de-journey

# ── Step 1: commit the step_3 fix on its branch ──────────────────────────────
git checkout sprint-01/day-04-logging
git add sprint-01/day-04/pipeline_log_demo.py
git status   # confirm only pipeline_log_demo.py is staged
git commit -m "[DAY-004][FIX] step_3 implemented — try/except/log.error/log.warning"
git push origin sprint-01/day-04-logging

# ── Step 2: merge all completed days into develop ────────────────────────────
git checkout develop
git pull origin develop

git merge sprint-01/day-01-env-setup      --no-edit
git merge sprint-01/day-02-schema-queries --no-edit
git merge sprint-01/day-03-pandas-intro   --no-edit
git merge sprint-01/day-04-logging        --no-edit
git push origin develop

# ── Step 3: stable snapshot onto main ────────────────────────────────────────
git checkout main
git pull origin main
git merge develop --no-edit
git push origin main

# ── Step 4: verify ───────────────────────────────────────────────────────────
git log --oneline -6

# ── Step 5: create today's branch from develop (ALWAYS from develop) ─────────
git checkout develop
git checkout -b sprint-01/day-05-git-workflow
```

### Day 04 Assessment
| Item | Result | Note |
|------|--------|------|
| logger.py module | ✅ Pass | Both stdlib + loguru factories working |
| db_utils.py upgraded | ✅ Pass | No print() remaining |
| step_3 error recovery | ✅ Pass | loguru diagnose=True captured variable values — excellent |
| JSON log file | ✅ Pass | All fields present, timezone-aware timestamp |
| Pipeline continues after error | ✅ Pass | WARNING fires, PIPELINE END fires |
| Git hygiene | ⚠️ Fixed today | Multiple commits per day — permanent fix applied above |

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-01: Environment & Foundations                             |
| Story           | ST-05: Git Workflow Automation + Daily Discipline            |
| Task ID         | TASK-005                                                     |
| Sprint          | Sprint 01 (Days 1–7)                                         |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | git, gitpython, automation, workflow, day-05                 |
| Acceptance Criteria | Git mental model clear; GitPython script automates daily workflow; replaces manual shell script; one clean commit per day from here on |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                        |
|---------------|----------------------------------------------|
| Branch        | `sprint-01/day-05-git-workflow`              |
| Base Branch   | `develop`                                    |
| Commit Prefix | `[DAY-005]`                                  |
| Folder        | `sprint-01/day-05/`                          |
| Files to Push | `git_workflow.py`, `daily_commit.py`, updated `scripts/daily_log.py` |

---

## 📚 BACKGROUND

### Why Git Went Wrong in Days 01–04 — The Mental Model

The confusion came from not having a clear picture of what each branch is FOR.
Here it is, once and for all:

```
BRANCH          PURPOSE                      WHO COMMITS DIRECTLY
──────────────────────────────────────────────────────────────────
main            Stable, sprint-complete code  NOBODY — merge only
develop         Integration branch            NOBODY — merge only
sprint-XX/dayYY Your daily workspace          YOU — one commit/day
```

```
DAILY LIFECYCLE (visualised)
─────────────────────────────────────────────────────────────────

  develop ──────────────────────────────────────────►
              │                              ▲
              │ checkout -b day-05           │ merge day-05
              ▼                              │
  day-05 ─────── commit ── commit(fix) ──────┘
                    ↑
              ONE commit per day
              (fix commits are OK, but squash before merge ideally)
```

### The Five Git Commands You Use Every Single Day

```bash
git status                    # what changed? (run this constantly)
git diff                      # what exactly changed line by line?
git add .                     # stage everything
git commit -m "message"       # one commit per day
git push -u origin BRANCH     # push to remote
```

### Commands you use weekly (end of sprint)

```bash
git checkout develop
git merge sprint-XX/day-YY    # bring day's work in
git push origin develop

git checkout main
git merge develop             # stable snapshot
git tag sprint-01-complete    # tag the sprint
git push origin main --tags
```

### Commands for mistakes (you'll need these)

```bash
# Undo last commit but KEEP changes (staged)
git reset --soft HEAD~1

# Undo last commit and DISCARD changes (dangerous)
git reset --hard HEAD~1

# Stash uncommitted changes temporarily
git stash
git stash pop                 # restore them

# See what's different between branches
git diff develop..sprint-01/day-05-git-workflow

# Rename a branch
git branch -m old-name new-name
git push origin :old-name new-name
```

---

## 🎯 OBJECTIVES

1. Solidify the daily git mental model — branch → work → merge → main
2. Install `GitPython` and understand its API
3. Build `daily_commit.py` — Python script that replaces all manual git commands
4. Upgrade `scripts/daily_log.py` to use GitPython instead of subprocess
5. Add `git diff` summary to daily log output
6. Run the full Day 05 workflow using your own automation script

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                       |
|-------|----------|------------------------------------------------|
| A     | 20 min   | Repo cleanup (retrospective above) + GitPython install |
| B     | 35 min   | `git_workflow.py` — GitPython fundamentals     |
| C     | 35 min   | `daily_commit.py` — full daily automation      |
| D     | 10 min   | Run Day 05 workflow using your own script      |
| E     | 20 min   | Merge to develop + push                        |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install GitPython + Fundamentals (Block A+B)
**[Full steps — new library, new API]**

```bash
# Activate venv and install
.venv\Scripts\activate
pip install GitPython==3.1.41

# Add to requirements.txt (uncomment or add):
echo GitPython==3.1.41 >> requirements.txt
```

Create `sprint-01/day-05/git_workflow.py`:

```python
#!/usr/bin/env python3
"""
git_workflow.py — Day 05 | GitPython Fundamentals
==================================================
Demonstrates the GitPython API used in daily_commit.py.
Run this to understand the library before using it in automation.

Run: python git_workflow.py
"""

import sys
from pathlib import Path
from datetime import datetime
from git import Repo, InvalidGitRepositoryError

sys.path.insert(0, str(Path(__file__).parent.parent / "day-04"))
from logger import get_pipeline_logger

logger = get_pipeline_logger("git_workflow")


def get_repo() -> Repo:
    """
    Find and return the Git repo starting from current file's location.
    Walks up directories until it finds .git folder.
    Raises InvalidGitRepositoryError if no repo found.
    """
    search_path = Path(__file__).resolve()
    for parent in [search_path] + list(search_path.parents):
        if (parent / ".git").exists():
            logger.info("Git repo found | path={}", parent)
            return Repo(parent)
    raise InvalidGitRepositoryError("No git repo found in path hierarchy")


def show_repo_status(repo: Repo) -> None:
    """
    Show current repo state — equivalent to running git status.
    """
    logger.info("── Repo Status ──────────────────────────")

    # Current branch
    try:
        branch = repo.active_branch.name
    except TypeError:
        branch = "detached HEAD"
    logger.info("Active branch     | {}", branch)

    # Tracking branch (remote)
    try:
        tracking = repo.active_branch.tracking_branch()
        logger.info("Tracking          | {}", tracking or "not set")
    except Exception:
        logger.info("Tracking          | not set")

    # Commit count
    commit_count = sum(1 for _ in repo.iter_commits())
    logger.info("Total commits     | {:,}", commit_count)

    # Latest commit
    latest = repo.head.commit
    logger.info("Latest commit     | {} | {} | {}",
                latest.hexsha[:8],
                latest.author.name,
                latest.message.strip()[:60])

    # Uncommitted changes
    changed_files  = [item.a_path for item in repo.index.diff(None)]     # modified
    staged_files   = [item.a_path for item in repo.index.diff("HEAD")]   # staged
    untracked      = repo.untracked_files                                  # new files

    logger.info("Modified (unstaged) | {} files: {}", len(changed_files), changed_files[:5])
    logger.info("Staged              | {} files: {}", len(staged_files),  staged_files[:5])
    logger.info("Untracked           | {} files: {}", len(untracked),     untracked[:5])


def show_branch_summary(repo: Repo) -> None:
    """
    List all branches and their latest commit.
    Equivalent to: git branch -a --verbose
    """
    logger.info("── Branch Summary ───────────────────────")

    # Local branches
    for branch in repo.branches:
        commit = branch.commit
        logger.info("LOCAL  {:45s} | {} | {}",
                    branch.name,
                    commit.hexsha[:8],
                    commit.message.strip()[:50])

    # Remote branches
    for ref in repo.remotes[0].refs if repo.remotes else []:
        logger.info("REMOTE {:45s} | {}",
                    ref.name,
                    ref.commit.hexsha[:8])


def show_recent_commits(repo: Repo, n: int = 5) -> None:
    """
    Show last N commits — equivalent to git log --oneline -N
    """
    logger.info("── Last {} Commits ──────────────────────", n)
    for commit in list(repo.iter_commits())[:n]:
        dt = datetime.fromtimestamp(commit.committed_date).strftime("%Y-%m-%d %H:%M")
        logger.info("{} | {} | {} | {}",
                    commit.hexsha[:8],
                    dt,
                    commit.author.name[:15],
                    commit.message.strip()[:55])


def show_diff_summary(repo: Repo) -> None:
    """
    Show what files changed since last commit and how many lines.
    Equivalent to: git diff --stat HEAD
    """
    logger.info("── Diff Summary (vs HEAD) ───────────────")
    try:
        diff = repo.head.commit.diff(None)   # None = working tree
        if not diff:
            logger.info("No changes since last commit")
            return
        for d in diff:
            change_type = d.change_type   # A=added, D=deleted, M=modified, R=renamed
            logger.info("{} | {}", change_type, d.a_path)
    except Exception as exc:
        logger.warning("Could not compute diff | {}", exc)


def main():
    logger.info("GitPython Fundamentals Demo")
    logger.info("=" * 50)

    try:
        repo = get_repo()
    except InvalidGitRepositoryError as exc:
        logger.error("Git repo not found | {}", exc)
        return

    show_repo_status(repo)
    show_branch_summary(repo)
    show_recent_commits(repo, n=5)
    show_diff_summary(repo)

    logger.info("=" * 50)
    logger.info("GitPython demo complete")


if __name__ == "__main__":
    main()
```

**✅ Checkpoint:** `python git_workflow.py` prints your repo state, all branches, and last 5 commits with no errors.

---

### EXERCISE 2 — daily_commit.py: Full Daily Automation (Block C)
**[Q1 fully provided. Q2 write yourself — hints given]**

**Objective:** Replace every manual git command in your daily workflow with one Python script.
After today, you run `python scripts/daily_commit.py --day 5 --message "..."` and it does
everything: stage → commit → push → merge → log.

Create `scripts/daily_commit.py`:

```python
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
            log.info("  git checkout -b sprint-{:02d}/day-{:02d}-your-topic", sprint, day)
            sys.exit(0)

    log.info("Branch | {}", branch)
    return branch


def stage_all(repo: Repo) -> list[str]:
    """
    Stage all changes (git add .).
    Returns list of staged file paths.
    """
    repo.git.add(A=True)       # equivalent to git add -A (all changes including deletions)
    staged = [item.a_path for item in repo.index.diff("HEAD")]
    untracked_count = len(repo.untracked_files)
    log.info("Staged | {} changed files, {} untracked", len(staged), untracked_count)
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

    commit = repo.index.commit(
        full_msg,
        author_date=datetime.now().astimezone().isoformat(),
        commit_date=datetime.now().astimezone().isoformat(),
    )
    log.info("Committed | {} | {}", commit.hexsha[:8], full_msg)
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
            log.info("Push | upstream set → origin/{}", branch)
        else:
            origin.push()
            log.info("Push | {} → origin/{}", branch, branch)
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
        log.info("Merging {} → develop", source_branch)

        repo.git.checkout("develop")
        repo.git.merge(source_branch, "--no-edit")

        origin = repo.remote("origin")
        origin.push()

        log.info("Merged and pushed develop ✅")

        # Return to original branch
        repo.git.checkout(source_branch)
        log.info("Returned to {}", source_branch)
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
            f.write("# 📓 90-Day Progress Log\n\n")
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
    log.info("Daily Commit | Day {:03d} | Sprint {:02d}", args.day, sprint)
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
    log.info("Day {:03d} complete | SHA={} | pushed={}", args.day, sha, pushed)
    log.info("=" * 52)


if __name__ == "__main__":
    main()
```

---

### EXERCISE 3 — Run Day 05 Using Your Own Script (Block D)
**[This is the payoff — your automation does the git work from now on]**

```bash
cd C:\Users\Lenovo\python-de-journey

# Make sure you are on the Day 05 branch
git checkout sprint-01/day-05-git-workflow

# Add your day 05 files
git add sprint-01/day-05/
git add scripts/daily_commit.py

# Use your OWN script to commit, push, AND merge to develop
python scripts/daily_commit.py \
    --day 5 \
    --sprint 1 \
    --message "Git workflow automation: GitPython fundamentals, daily_commit.py" \
    --merge

# Verify
git log --oneline -5
git branch -v | grep develop
```

**✅ Expected output:**
```
INFO | Branch | sprint-01/day-05-git-workflow
INFO | Staged | X changed files, Y untracked
INFO | Committed | abc1234 | [DAY-005][S01] Git workflow automation...
INFO | Push | sprint-01/day-05-git-workflow → origin/...
INFO | Merging sprint-01/day-05-git-workflow → develop
INFO | Merged and pushed develop ✅
INFO | Progress log updated → logs/progress.md
INFO | Day 005 complete | SHA=abc1234 | pushed=True
```

---

### EXERCISE 4 — Sprint End Preparation (Block E)
**[Run after implementing merge_to_main()]**

After implementing `merge_to_main()`, test it:

```bash
# Test --to-main (only run this at end of Sprint 01, Day 07)
# For now just verify the function exists and gives NotImplementedError gracefully:
python scripts/daily_commit.py \
    --day 5 --sprint 1 \
    --message "test merge_to_main" \
    --to-main

# Expected: WARNING "merge_to_main not implemented yet" — not a crash
```

---

## ✅ DAY 05 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | Repo cleanup complete — all days merged to develop + main             | [ ]   |
| 2 | `sprint-01/day-05-git-workflow` branch created from develop           | [ ]   |
| 3 | `GitPython==3.1.41` installed + in requirements.txt                   | [ ]   |
| 4 | `git_workflow.py` runs — shows repo status, branches, 5 commits       | [ ]   |
| 5 | `daily_commit.py` created in `scripts/`                               | [ ]   |
| 6 | `validate_branch()` warns when on main/develop                        | [ ]   |
| 7 | `make_commit()` formats message as `[DAY-005][S01] ...`               | [ ]   |
| 8 | `push_branch()` sets upstream on first push                           | [ ]   |
| 9 | `merge_to_develop()` merges and pushes develop                        | [ ]   |
|10 | **`merge_to_main()` written by you — tag created, returns to develop**| [ ]   |
|11 | Day 05 committed using `daily_commit.py --merge` (not manual git)     | [ ]   |
|12 | `logs/progress.md` has Day 05 entry with SHA                          | [ ]   |
|13 | Exactly one commit on the day-05 branch                               | [ ]   |

---

## 🔍 SELF-CHECK — merge_to_main() is correct when:

```bash
# Run at sprint end (Day 07 this week, but test the logic now):
python scripts/daily_commit.py --day 5 --sprint 1 \
    --message "sprint end test" --to-main

# Then verify:
git checkout main
git log --oneline -3
# → Should show merge commit from develop

git tag
# → Should list: sprint-01-complete

git checkout develop   # always return here
```

---

## 📌 GIT RULES — PERMANENT (Pinned for rest of 90 days)

```
RULE 1: Never commit directly to main or develop
RULE 2: One commit per day on feature branch
         (fix commits allowed, but squash if more than 2)
RULE 3: Always merge feature branch → develop at end of day
RULE 4: Always merge develop → main at end of each sprint
RULE 5: Use daily_commit.py --merge for rules 2+3 in one command
RULE 6: Commit message format: [DAY-XXX][S0Y][FIX?] Description
```

---

## 🔜 PREVIEW: DAY 06

**Topic:** Python logging automation — extend `daily_commit.py` to post progress
to a JIRA board via Python's `jira` SDK. Auto-create JIRA task, log time, update status.
Also introduces `configparser` and `pydantic` for config validation —
preview of the config-driven pipeline pattern in Sprint 08.

---

*Day 05 | Sprint 01 | EP-01 | TASK-005*
