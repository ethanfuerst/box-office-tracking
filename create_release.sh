#!/usr/bin/env bash
set -euo pipefail

# Ensure we're on main
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [[ "$current_branch" != "main" ]]; then
  echo "Error: you are on branch '$current_branch', not 'main'."
  exit 1
fi

# Ensure working tree is clean
if [[ -n "$(git status --porcelain)" ]]; then
  echo "Error: working tree is not clean. Commit or stash changes first."
  exit 1
fi

# Make sure main is up to date
echo "Updating main from origin..."
git pull --ff-only

# Check that gh CLI is installed
if ! command -v gh >/dev/null 2>&1; then
  echo "Error: gh (GitHub CLI) is not installed."
  echo "Install it with: brew install gh"
  exit 1
fi

echo
echo "Select version option:"
echo "  1) bump patch  (x.y.z -> x.y.(z+1))"
echo "  2) bump minor  (x.y.z -> x.(y+1).0)"
echo "  3) bump major  (x.y.z -> (x+1).0.0)"
echo "  4) set exact version"
echo "  5) do NOT bump (tag current version)"
read -rp "Choice [1-5]: " choice

did_bump=false

case "$choice" in
  1)
    echo "Bumping patch version with uv..."
    uv version --bump patch
    did_bump=true
    ;;
  2)
    echo "Bumping minor version with uv..."
    uv version --bump minor
    did_bump=true
    ;;
  3)
    echo "Bumping major version with uv..."
    uv version --bump major
    did_bump=true
    ;;
  4)
    read -rp "Enter exact version (e.g. 1.2.3): " exact
    if [[ -z "$exact" ]]; then
      echo "Error: empty version."
      exit 1
    fi
    echo "Setting version to $exact with uv..."
    uv version "$exact"
    did_bump=true
    ;;
  5)
    echo "No bump selected. Will tag current version from pyproject.toml."
    did_bump=false
    ;;
  *)
    echo "Error: invalid choice."
    exit 1
    ;;
esac

VERSION=$(uv version --short)
echo "Version to release is: v$VERSION"

# If we are not bumping, ensure there is no existing tag for this version
if [[ "$did_bump" == "false" ]]; then
  if git rev-parse "v$VERSION" >/dev/null 2>&1; then
    echo "Error: tag v$VERSION already exists. Refusing to create duplicate tag."
    exit 1
  fi
fi

# If we bumped, commit the version bump
if [[ "$did_bump" == "true" ]]; then
  # Stage version files
  if [[ -f uv.lock ]]; then
    git add pyproject.toml uv.lock
  else
    git add pyproject.toml
  fi

  # Confirm there is something to commit
  if [[ -n "$(git diff --cached --name-only)" ]]; then
    git commit -m "chore: release v$VERSION"
  else
    echo "Warning: no staged changes after bump; skipping commit."
  fi
fi

# Create tag and push
git tag "v$VERSION"
git push origin main
git push origin "v$VERSION"

# Create GitHub Release using gh
echo
read -rp "Create GitHub release v$VERSION with gh? [y/N]: " create_release
if [[ "$create_release" == "y" || "$create_release" == "Y" ]]; then
  gh release create "v$VERSION" \
    --title "v$VERSION" \
    --notes "Release v$VERSION"
fi

echo "Done. Released v$VERSION."
