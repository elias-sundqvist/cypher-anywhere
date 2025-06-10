#!/bin/bash
set -e
CORE_VERSION=$(npm version patch --workspace packages/core --no-workspaces-update --no-git-tag-version | tail -n1)
CORE_VERSION=${CORE_VERSION#v}
for pkg in packages/adapters/json-adapter packages/adapters/sqljs-adapter packages/adapters/sqljs-schema-adapter; do
  npm pkg set "dependencies.@cypher-anywhere/core"="$CORE_VERSION" -w "$pkg"
  npm version patch --workspace "$pkg" --no-workspaces-update --no-git-tag-version
done
git add packages/core/package.json packages/adapters/*/package.json
git commit -m "chore: bump versions to $CORE_VERSION" && git tag "v$CORE_VERSION" || echo "No version changes"
