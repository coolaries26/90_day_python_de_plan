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