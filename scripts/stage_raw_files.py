"""Fetches all four source datasets from the public internet and stages them into a Unity
Catalog Volume via the Databricks CLI.

This runs OUTSIDE Databricks (locally, or as a scheduled CI job -- see
resources/.github/workflows for the CI-triggered version) and is a hard architectural
requirement, not a style choice: Databricks Free Edition's serverless compute has no
outbound internet egress. A first version of the Bronze ingestion jobs tried to download
their source files from inside the job itself and failed with a DNS resolution error on a
real run (see docs/02_environment_and_branching.md). This script is what makes the
already-staged-file assumption in bronze/ingest_*.py true.

Requires the Databricks CLI to be installed and authenticated (this shells out to
`databricks fs cp`, rather than reimplementing Volume upload over the SDK, to reuse the
CLI's existing auth/profile handling).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aml_lakehouse.common.download import download_to_path  # noqa: E402

OFAC_BASE_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports"
OFAC_FILES = ("SDN.CSV", "ALT.CSV", "ADD.CSV", "SDN_COMMENTS.CSV")

OPENSANCTIONS_COLLECTIONS = ("sanctions", "peps")

AMLSIM_URL = "https://raw.githubusercontent.com/IBM/AMLSim/master/sample/20K_fanin200cycle200.tgz"

KAGGLE_ELLIPTIC_DATASET = "ellipticco/elliptic-data-set"


def _fs_cp(local_path: Path, volume_path: str, profile: str | None) -> None:
    # profile=None relies on DATABRICKS_HOST/DATABRICKS_TOKEN env vars instead of a named
    # ~/.databrickscfg profile -- that's how CI runs this (a fresh runner has no profile
    # file at all), whereas local/interactive use passes a real profile name.
    command = ["databricks", "fs", "cp", "--overwrite", str(local_path), f"dbfs:{volume_path}"]
    if profile:
        command += ["--profile", profile]
    subprocess.run(command, check=True)


def stage_ofac(tmp_dir: Path, catalog: str, profile: str | None) -> None:
    for filename in OFAC_FILES:
        local_path = tmp_dir / filename
        download_to_path(f"{OFAC_BASE_URL}/{filename}", str(local_path))
        _fs_cp(local_path, f"/Volumes/{catalog}/bronze/raw_files/ofac/{filename}", profile)


def stage_opensanctions(tmp_dir: Path, catalog: str, profile: str | None) -> None:
    for collection in OPENSANCTIONS_COLLECTIONS:
        filename = f"{collection}_targets_simple.csv"
        local_path = tmp_dir / filename
        url = f"https://data.opensanctions.org/datasets/latest/{collection}/targets.simple.csv"
        download_to_path(url, str(local_path))
        _fs_cp(local_path, f"/Volumes/{catalog}/bronze/raw_files/opensanctions/{filename}", profile)


def stage_amlsim(tmp_dir: Path, catalog: str, profile: str | None) -> None:
    tarball_path = tmp_dir / "amlsim.tgz"
    download_to_path(AMLSIM_URL, str(tarball_path))
    with tarfile.open(tarball_path, "r:gz") as tar:
        tar.extractall(path=tmp_dir)  # noqa: S202 -- trusted, pinned source URL
    dataset_dir = tmp_dir / "20K_fanin200cycle200"
    for filename in ("nodes.csv", "transactions.csv"):
        _fs_cp(dataset_dir / filename, f"/Volumes/{catalog}/bronze/raw_files/amlsim/{filename}", profile)


def stage_elliptic(tmp_dir: Path, catalog: str, profile: str | None) -> None:
    import kagglehub  # optional dependency, see pyproject.toml's "elliptic" extra

    cache_path = Path(kagglehub.dataset_download(KAGGLE_ELLIPTIC_DATASET))
    dataset_dir = cache_path / "elliptic_bitcoin_dataset"
    for filename in ("elliptic_txs_classes.csv", "elliptic_txs_edgelist.csv", "elliptic_txs_features.csv"):
        _fs_cp(dataset_dir / filename, f"/Volumes/{catalog}/bronze/raw_files/elliptic/{filename}", profile)


STAGERS = {
    "ofac": stage_ofac,
    "opensanctions": stage_opensanctions,
    "amlsim": stage_amlsim,
    "elliptic": stage_elliptic,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="e.g. aml_dev")
    parser.add_argument(
        "--profile",
        default=None,
        help="~/.databrickscfg profile (omit to use DATABRICKS_HOST/DATABRICKS_TOKEN env vars, as CI does)",
    )
    parser.add_argument(
        "--sources", nargs="+", choices=list(STAGERS), default=list(STAGERS),
        help="which sources to stage (default: all)",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        for source in args.sources:
            print(f"staging {source}...")
            STAGERS[source](tmp_dir, args.catalog, args.profile)
            print(f"staged {source}")


if __name__ == "__main__":
    main()
