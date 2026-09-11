## Step 2 — history purge (maintainer with admin only)

The key sits in the history of `lib/capture_export.py` and `lib/grower_export.py`, from commit `60220cf2` (2021-12-20). This repository has no tags. You must rewrite the history and force-push.

**Requirements on your machine:** `git` (2.24 or newer), `python3`, and `curl`. You do not need to install `git-filter-repo`; Block 1 downloads it.

Run the blocks in order, in one terminal session.

### Block 1 — redact and verify locally (safe; does not push)

This block finds the key itself and replaces every copy with `REDACTED`. You do not paste any secret. It targets only the value in the `X-CKAN-API-Key` header, so it does not touch the CKAN package or dataset ids (which are also UUIDs, but are not secrets).

```bash
set -euo pipefail
REPO=treetracker-airflow-dags
URL=https://github.com/Greenstand/$REPO
WORK=/tmp/purge-$REPO
FILES="lib/capture_export.py lib/grower_export.py"

rm -rf "$WORK" && mkdir -p "$WORK"
curl -fsSL https://raw.githubusercontent.com/newren/git-filter-repo/main/git-filter-repo -o "$WORK/git-filter-repo"
chmod +x "$WORK/git-filter-repo"

git clone --mirror "$URL" "$WORK/$REPO.git"
cd "$WORK/$REPO.git"

# Extract only the UUID that follows an X-CKAN-API-Key header, across all history.
# This avoids the package/dataset UUIDs, which are not secrets.
git grep -hoE "X-CKAN-API-Key[\"']?[[:space:]]*:[[:space:]]*[\"'][0-9a-fA-F-]{20,}" $(git rev-list --all) -- $FILES \
  | grep -oE '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' \
  | sort -u > "$WORK/keys.txt"
echo "Found $(wc -l < "$WORK/keys.txt") CKAN key value(s) to redact."
test -s "$WORK/keys.txt" || { echo "No key found; stop and investigate."; exit 1; }

sed 's/$/==>REDACTED/' "$WORK/keys.txt" > "$WORK/replacements.txt"
python3 "$WORK/git-filter-repo" --replace-text "$WORK/replacements.txt"

# Local verify: no key remains on any ref
fail=0
while read -r k; do
  [ -n "$(git log --all -S"$k" --oneline)" ] && { echo "FAIL: the key is still present"; fail=1; }
done < "$WORK/keys.txt"
[ "$fail" -eq 0 ] && echo "OK: the key is removed from history." || exit 1
echo "Rewritten repo is at $WORK/$REPO.git . Review it, then lift branch protection and run Block 2."
```

### Manual gate A — lift branch protection

Lift the branch protection on `main` in the GitHub UI (Settings → Branches), or with your admin token. Record the current rules first, so you can restore them. A protected branch rejects the force-push.

### Block 2 — push the rewrite

```bash
set -euo pipefail
REPO=treetracker-airflow-dags
WORK=/tmp/purge-$REPO
cd "$WORK/$REPO.git"

# git-filter-repo removes the origin remote after the rewrite; add it back.
git remote add origin https://github.com/Greenstand/$REPO
git push --force --all
git push --force --tags
echo "Pushed. Now restore branch protection (Manual gate B), then run Block 3."
```

### Manual gate B — restore branch protection

Restore the branch protection on `main` with the same rules you recorded in gate A.

### Block 3 — verify on a fresh clone (done-bar)

```bash
set -euo pipefail
REPO=treetracker-airflow-dags
WORK=/tmp/purge-$REPO
V=/tmp/verify-$REPO
rm -rf "$V"
# Use a NORMAL clone, not --mirror. A mirror clone also fetches refs/pull/*,
# which are GitHub-managed read-only refs you cannot rewrite. They would give
# a false failure. A normal clone checks the branches and tags you control.
git clone https://github.com/Greenstand/$REPO "$V" >/dev/null 2>&1
cd "$V"
fail=0
while read -r k; do
  [ -n "$(git log --all -S"$k" --oneline)" ] && { echo "FAIL"; fail=1; }
done < "$WORK/keys.txt"
[ "$fail" -eq 0 ] && echo "PASS: key gone from branches and tags" || exit 1
```

Verification trap to avoid: do not use `git clone --mirror` to verify. It fetches the `refs/pull/*` refs, which you cannot change, so it always shows the key and reports a false failure.

**Close the open pull requests.** This repository has 4 open PRs (#258, #239, #226, #111). A rewrite changes every commit SHA, so their bases become invalid even though none touch the two files. Close them and ask the authors to re-create against the rewritten base. After the push, tell contributors to re-clone or run `git reset --hard`.

## Step 3 — residual exposure (maintainer with admin only; manual)

GitHub keeps old commits alive in three places you cannot reach with a push: the `refs/pull/*` refs (about 192 pull requests, open and closed), the cached commit views, and the ~24 forks. The key is dead, but do the best-effort cleanup:

1. Open a GitHub Support request for this repository. Ask Support to remove the `refs/pull/*/head` copies of the key, to purge the cached commit views, and to remove the dangling commits from the shared fork-network storage.
2. Send a short notice to the fork owners (24). Ask them to re-sync or delete the fork.
3. Accept the remaining risk. A fork owner who does not act may still serve the dead key from the fork. We record this as an accepted risk.

## Scope

- Rotation is done offline. This PR does not cover rotation.
- `DB_URL` is env-sourced and never committed, so it is not a purge target.
- Recurrence prevention is out of scope. Note: `lib/capture_export.py` has a `print("DB_URL:", DB_URL)` line that can leak the connection string to runtime logs; removing it is a separate hygiene fix.
