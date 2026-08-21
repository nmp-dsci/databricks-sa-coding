#!/usr/bin/env python
"""Fast-forward the workspace Git folder to the tip of the tracked branch.

A Databricks Git folder is a *clone*, not a live view. It pins the commit it was
created at and does not follow the remote on its own — a fresh `repos create`
lands on whatever `main` pointed at when the credential last cached it, which is
not necessarily the commit you just pushed. This pulls it forward.

Run it after any push, so the notebooks you open in the workspace UI are the
notebooks in the repo. `make deploy` does NOT do this: the bundle uploads files
to its own `.bundle/` path, which is a separate copy from the Git folder.

Creates the folder if it does not exist yet.
"""

from __future__ import annotations

import os
import subprocess
import sys

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
except ImportError:  # pragma: no cover
    sys.exit("databricks-sdk not installed — run `uv sync --group dev`")

PROFILE = os.environ.get("PROFILE") or os.environ.get("DATABRICKS_CONFIG_PROFILE", "nmp-dsci")
REPO_URL = "https://github.com/nmp-dsci/databricks-sa-coding"
BRANCH = os.environ.get("BRANCH", "main")


def local_head() -> str | None:
    """The commit the local checkout is on, for comparison against the remote."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", BRANCH], capture_output=True, text=True, check=True
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def main() -> int:
    client = WorkspaceClient(profile=PROFILE)
    user = client.current_user.me().user_name
    path = f"/Users/{user}/databricks-sa-coding"

    # Look the folder up by PATH, not with `repos.list()`. On this workspace
    # `repos.list()` returns an empty page even when a folder exists — with or
    # without a path_prefix — so trusting it means re-creating a folder that is
    # already there on every run. `workspace.get_status` reports the same
    # object's id reliably, and flags whether it is a Git folder.
    repo_id = None
    try:
        status = client.workspace.get_status(path)
        info = getattr(status, "directory_info", None)
        if info is not None and getattr(info, "is_git_folder", False):
            repo_id = status.object_id
        elif status is not None:
            sys.exit(f"{path} exists but is not a Git folder — move it aside and re-run")
    except NotFound:
        pass

    if repo_id is None:
        print(f"no Git folder at {path} — creating it")
        folder = client.repos.create(url=f"{REPO_URL}.git", provider="gitHub", path=path)
    else:
        # Re-checking out the branch is what triggers the pull. There is no
        # separate "fetch" operation on the Repos API.
        client.repos.update(repo_id=repo_id, branch=BRANCH)
        folder = client.repos.get(repo_id)

    remote_head = folder.head_commit_id or "unknown"
    print(f"workspace: {folder.path} @ {folder.branch} -> {remote_head[:12]}")

    head = local_head()
    if head is None:
        print("(not a git checkout here — skipping the comparison)")
    elif head == remote_head:
        print(f"in sync with local {BRANCH} ({head[:12]})")
    else:
        # Almost always means a local commit has not been pushed yet: the folder
        # tracks the *remote*, so it can never be ahead of origin.
        print(f"OUT OF SYNC — local {BRANCH} is {head[:12]}, workspace is {remote_head[:12]}")
        print("  push first (`git push origin main`), then re-run this.")
        return 1

    print(f"open: {client.config.host}/#workspace{folder.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
