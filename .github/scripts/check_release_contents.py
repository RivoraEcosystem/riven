from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = ROOT / ".github" / "release-files.json"


def report_difference(label: str, expected: set[str], actual: set[str]) -> bool:
    extra = sorted(actual - expected)
    missing = sorted(expected - actual)
    kind = "directories" if label.lower().endswith("directories") else "files"
    print(f"{label}: Missing {kind}: {len(missing)}; Extra {kind}: {len(extra)}")
    for path in missing:
        print(f"  Missing: {path}", file=sys.stderr)
    for path in extra:
        print(f"  Extra: {path}", file=sys.stderr)
    return not missing and not extra


def expected_directories(paths: set[str], prefix: str = "") -> set[str]:
    directories: set[str] = set()
    for filename in paths:
        parent = PurePosixPath(filename).parent
        while parent != PurePosixPath("."):
            path = parent.as_posix()
            directories.add(f"{prefix}{path}")
            parent = parent.parent
    return directories


def check_wheel(wheel_path: Path, allowed: set[str]) -> bool:
    try:
        with zipfile.ZipFile(wheel_path) as archive:
            entries = archive.infolist()
    except (OSError, zipfile.BadZipFile) as exc:
        print(f"Cannot read wheel {wheel_path}: {exc}", file=sys.stderr)
        return False
    names = [entry.filename for entry in entries]
    if len(names) != len(set(names)):
        print("Wheel contains duplicate paths", file=sys.stderr)
        return False
    for entry in entries:
        mode = entry.external_attr >> 16
        if entry.is_dir() or stat.S_ISLNK(mode):
            print(f"Unsupported wheel member: {entry.filename}", file=sys.stderr)
            return False
    actual_files = set(names)
    actual_directories = expected_directories(actual_files)
    return report_difference("Wheel files", allowed, actual_files) and report_difference(
        "Wheel directories", expected_directories(allowed), actual_directories
    )


def check_sdist(sdist_path: Path, allowed: set[str], root_name: str) -> bool:
    try:
        with tarfile.open(sdist_path, "r:gz") as archive:
            members = archive.getmembers()
    except (OSError, tarfile.TarError) as exc:
        print(f"Cannot read source distribution {sdist_path}: {exc}", file=sys.stderr)
        return False

    files: set[str] = set()
    directories: set[str] = set()
    member_names: list[str] = []
    for member in members:
        member_names.append(member.name)
        path = PurePosixPath(member.name)
        if not path.parts or path.parts[0] != root_name or path.is_absolute() or ".." in path.parts:
            print(f"Invalid source distribution path: {member.name}", file=sys.stderr)
            return False
        if member.isfile():
            files.add(path.relative_to(root_name).as_posix())
        elif member.isdir():
            directories.add(path.as_posix())
        else:
            print(f"Unsupported source distribution member: {member.name}", file=sys.stderr)
            return False
    if len(member_names) != len(set(member_names)):
        print("Source distribution contains duplicate paths", file=sys.stderr)
        return False

    allowed_directories = {root_name} | expected_directories(allowed, prefix=f"{root_name}/")
    return report_difference("Source distribution files", allowed, files) and report_difference(
        "Source distribution directories", allowed_directories, directories
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate release artifact contents.")
    parser.add_argument("--dist-dir", type=Path, default=Path("dist"))
    args = parser.parse_args()

    with (ROOT / "pyproject.toml").open("rb") as project_file:
        project = tomllib.load(project_file)["project"]
    version = project["version"]
    normalized_name = re.sub(r"[-_.]+", "_", project["name"]).lower()
    expected_tag = f"v{version}"
    release_tag = os.environ.get("RELEASE_TAG")
    if release_tag and release_tag != expected_tag:
        print(
            f"Release tag {release_tag!r} does not match package version tag {expected_tag!r}",
            file=sys.stderr,
        )
        return 1

    wheel_name = f"{normalized_name}-{version}-py3-none-any.whl"
    sdist_name = f"{normalized_name}-{version}.tar.gz"
    try:
        entries = list(args.dist_dir.iterdir())
    except OSError as exc:
        print(f"Cannot read distribution directory {args.dist_dir}: {exc}", file=sys.stderr)
        return 1
    expected_artifacts = {wheel_name, sdist_name}
    dist_files = {path.name for path in entries}
    regular_files = all(path.is_file() and not path.is_symlink() for path in entries)
    if dist_files != expected_artifacts or not regular_files:
        report_difference("distribution artifacts", expected_artifacts, dist_files)
        if not regular_files:
            print("Distribution directory may contain only regular files", file=sys.stderr)
        return 1

    allowlist = json.loads(ALLOWLIST.read_text(encoding="utf-8"))
    dist_info = f"{normalized_name}-{version}.dist-info"
    egg_info = f"{normalized_name}.egg-info"
    wheel_allowed = {item.format(dist_info=dist_info) for item in allowlist["wheel"]}
    sdist_allowed = {item.format(version=version, egg_info=egg_info) for item in allowlist["sdist"]}
    wheel_ok = check_wheel(args.dist_dir / wheel_name, wheel_allowed)
    sdist_ok = check_sdist(args.dist_dir / sdist_name, sdist_allowed, f"{normalized_name}-{version}")
    if wheel_ok and sdist_ok:
        print("Release artifact contents match .github/release-files.json")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
