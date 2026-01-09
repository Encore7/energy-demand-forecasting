#!/bin/sh
set -euo pipefail

LAKEFS_ENDPOINT="${LAKEFS_ENDPOINT}"
LAKEFS_ACCESS_KEY_ID="${LAKEFS_ACCESS_KEY_ID}"
LAKEFS_SECRET_ACCESS_KEY="${LAKEFS_SECRET_ACCESS_KEY}"
REPO="${LAKEFS_REPO}"
DEFAULT_BRANCH="${LAKEFS_BRANCH}"
S3_BUCKET="${S3_BUCKET}"

echo ">> Creating lakeFS repo '${REPO}' (if not exists) at ${LAKEFS_ENDPOINT}"

curl -sS -u "${LAKEFS_ACCESS_KEY_ID}:${LAKEFS_SECRET_ACCESS_KEY}" \
  -H "Content-Type: application/json" \
  -X POST "${LAKEFS_ENDPOINT}/api/v1/repositories" \
  -d "{
    \"name\": \"${REPO}\",
    \"storage_namespace\": \"s3://${S3_BUCKET}\",
    \"default_branch\": \"${DEFAULT_BRANCH}\"
  }" \
  || echo ">> Repo may already exist, ignoring."

echo ">> Done. Repo=${REPO}, branch=${DEFAULT_BRANCH}"
