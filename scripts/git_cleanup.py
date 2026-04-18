#!/usr/bin/env python3
"""
git_cleanup.py — Safe Git History Cleanup Tool
Usage: python scripts/git_cleanup.py
"""
import subprocess
import sys

def run(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout.strip())
    if result.stderr:
        print("ERROR:", result.stderr.strip())
    return result.returncode == 0

print("=== Git History Cleanup Tool ===\n")

branch = subprocess.getoutput("git branch --show-current")
print(f"Current branch → {branch}\n")

print("Recent commits:")
run("git log --oneline -15")

n = input("\nHow many recent commits do you want to clean? (e.g. 10): ").strip()

if not n.isdigit():
    print("Invalid number. Aborting.")
    sys.exit(1)

print(f"\nStarting interactive rebase on last {n} commits...\n")
run(f"git rebase -i HEAD~{n}")

print("\n✅ Rebase completed.")
print(f"\nTo push the cleaned history safely, run:")
print(f"   git push origin {branch} --force-with-lease")