"""Route Conan recipe source archives through the ServiceGen Nexus proxy."""

from __future__ import annotations

import os
from urllib.parse import urlsplit, urlunsplit

from conan.errors import ConanException


_REPOSITORIES = {
    "archives.boost.io": "conan-source-archives-boost",
    "cmake.org": "conan-source-cmake",
    "curl.se": "conan-source-curl",
    "dist.schmorp.de": "conan-source-schmorp",
    "distfiles.ariadne.space": "conan-source-ariadne",
    "ftp.gnu.org": "conan-source-gnu-ftp",
    "ftpmirror.gnu.org": "conan-source-gnu-mirror",
    "github.com": "github-raw",
    "https.git.savannah.gnu.org": "conan-source-savannah-git",
    "mirrors.kernel.org": "conan-source-kernel",
    "sourceforge.net": "conan-source-sourceforge",
    "sourceware.org": "conan-source-sourceware",
    "www.mirrorservice.org": "conan-source-mirrorservice",
    "zlib.net": "conan-source-zlib",
}


def _proxy_root() -> str | None:
    explicit = os.getenv("SERVICEGEN_CONAN_SOURCE_PROXY_BASE", "").rstrip("/")
    if explicit:
        return explicit
    conan_remote = os.getenv("SERVICEGEN_CONAN_REMOTE_URL", "").rstrip("/")
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
    repository = _REPOSITORIES.get(parsed.hostname or "")
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
