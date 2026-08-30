"""Route Conan recipe source archives through the ServiceGen Nexus proxy."""

from __future__ import annotations

import os
import json
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from conan.errors import ConanException


def _repositories() -> dict[str, str]:
    catalog_path = Path(__file__).with_name("source-proxies.generated.json")
    try:
        catalog = json.loads(catalog_path.read_text())
    except (OSError, ValueError) as error:
        raise ConanException(
            f"Conan source proxy catalog is unavailable: {catalog_path}: {error}"
        ) from error
    return {host: proxy["repository"] for host, proxy in catalog.items()}


def _proxy_root() -> str | None:
    explicit = os.getenv("DEPENDENCY_CONAN_SOURCE_PROXY_BASE", "").rstrip("/")
    if explicit:
        return explicit
    conan_remote = os.getenv("DEPENDENCY_CONAN_REMOTE_URL", "").rstrip("/")
    if conan_remote.endswith("/conan-proxy"):
        return conan_remote.rsplit("/", 1)[0]
    return None


def _rewrite(value, proxy_root: str, conanfile):
    if isinstance(value, dict):
        for key, item in value.items():
            value[key] = _rewrite(item, proxy_root, conanfile)
        return value
    if isinstance(value, list):
        for index, item in enumerate(value):
            value[index] = _rewrite(item, proxy_root, conanfile)
        return value
    if not isinstance(value, str) or not value.startswith(("http://", "https://")):
        return value

    parsed = urlsplit(value)
    repository = _repositories().get(parsed.hostname or "")
    if repository is None:
        raise ConanException(
            "Conan source host is not configured in the ServiceGen dependency "
            f"proxy: {parsed.hostname!r} ({value}). Add an immutable Nexus raw "
            "proxy mapping before updating the dependency."
        )
    rewritten = urlunsplit(
        urlsplit(f"{proxy_root}/{repository}{parsed.path}")._replace(
            query=parsed.query,
            fragment=parsed.fragment,
        )
    )
    conanfile.output.info(f"Source URL routed through ServiceGen Nexus: {rewritten}")
    return rewritten


def pre_source(conanfile) -> None:
    proxy_root = _proxy_root()
    if proxy_root is None:
        return
    sources = (conanfile.conan_data or {}).get("sources", {})
    version_sources = sources.get(str(conanfile.version))
    if version_sources is not None:
        _rewrite(version_sources, proxy_root, conanfile)
