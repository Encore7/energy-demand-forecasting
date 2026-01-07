set -euo pipefail

LAKEFS_ENDPOINT="${LAKEFS_ENDPOINT:-http://localhost:8000}"
LAKEFS_ACCESS_KEY_ID="${LAKEFS_ACCESS_KEY_ID:-AKIADEV}"
LAKEFS_SECRET_ACCESS_KEY="${LAKEFS_SECRET_ACCESS_KEY:-SECRETDEV}"

REPO="${LAKEFS_REPO:-energy}"
DEFAULT_BRANCH="${LAKEFS_BRANCH:-main}"

# MinIO backend where lakeFS stores objects
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-lakefs-data}"

echo ">> Creating lakeFS repo '${REPO}' (if not exists) at ${LAKEFS_ENDPOINT}"

curl -sS -u "${LAKEFS_ACCESS_KEY_ID}:${LAKEFS_SECRET_ACCESS_KEY}" \
  -H "Content-Type: application/json" \
  -X POST "${LAKEFS_ENDPOINT}/api/v1/repositories" \
  -d "{
    \"name\": \"${REPO}\",
    \"storage_namespace\": \"s3://${S3_BUCKET}/${REPO}\",
    \"default_branch\": \"${DEFAULT_BRANCH}\"
  }" \
  || true

echo ">> Done. Repo=${REPO}, branch=${DEFAULT_BRANCH}"
