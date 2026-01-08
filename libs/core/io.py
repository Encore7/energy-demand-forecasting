from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LakeFSLocation:
    """
    LakeFS location helper.

    - For Spark (s3a):   s3a://{repo}/{branch}/{path}
    - For s3fs/pyarrow:  {repo}/{branch}/{path}
    - For boto3 keys:    {branch}/{path}
    """

    repo: str
    branch: str

    def uri(self, path: str) -> str:
        """Spark-friendly S3A URI."""
        path = path.lstrip("/")
        return f"s3a://{self.repo}/{self.branch}/{path}"

    def dataset_path(self, path: str) -> str:
        """Path for pyarrow.dataset(..., filesystem=s3fs)."""
        path = path.lstrip("/")
        return f"{self.repo}/{self.branch}/{path}"

    def key(self, path: str) -> str:
        """Key for boto3 S3 operations (Bucket=self.repo, Key=...)."""
        path = path.lstrip("/")
        return f"{self.branch}/{path}"
