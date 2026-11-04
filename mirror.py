import concurrent.futures as cf
import hashlib
import logging
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import yaml

UPSTREAM = os.environ.get("UPSTREAM", "https://helm.releases.hashicorp.com").rstrip("/") + "/"
OUT = Path(os.environ.get("OUT_DIR", "public"))
CHARTS = {c.strip() for c in os.environ.get("CHARTS", "").split(",") if c.strip()}
KEEP_LAST = int(os.environ.get("KEEP_LAST", "0"))
WORKERS = int(os.environ.get("WORKERS", "8"))
RETRIES = 3

log = logging.getLogger("mirror")


class StrTimestampLoader(yaml.SafeLoader):
    pass


StrTimestampLoader.add_constructor(
    "tag:yaml.org,2002:timestamp", lambda loader, node: loader.construct_scalar(node)
)


def fetch(url: str) -> bytes:
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "helm-mirror/1.0"})
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.read()
        except Exception as e:
            last = e
            log.warning("fetch failed (%d/%d) %s: %s", attempt, RETRIES, url, e)
    raise RuntimeError(f"cannot fetch {url}") from last


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: Path, digest: str | None) -> str:
    if dest.exists() and (not digest or sha256(dest) == digest):
        return "cached"
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_bytes(fetch(url))
    if digest and sha256(tmp) != digest:
        tmp.unlink()
        raise ValueError(f"digest mismatch: {url}")
    tmp.replace(dest)
    return "downloaded"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    OUT.mkdir(parents=True, exist_ok=True)

    index = yaml.load(fetch(UPSTREAM + "index.yaml"), Loader=StrTimestampLoader)
    entries = index.get("entries") or {}
    if CHARTS:
        missing = CHARTS - entries.keys()
        if missing:
            log.warning("charts not found upstream: %s", ", ".join(sorted(missing)))
        entries = {k: v for k, v in entries.items() if k in CHARTS}
    if KEEP_LAST > 0:
        entries = {k: v[:KEEP_LAST] for k, v in entries.items()}

    jobs = {}
    for name, versions in entries.items():
        for v in versions:
            urls = v.get("urls") or []
            if not urls:
                log.warning("no urls: %s %s", name, v.get("version"))
                continue
            src = urllib.parse.urljoin(UPSTREAM, urls[0])
            filename = Path(urllib.parse.urlparse(src).path).name
            if filename in jobs and jobs[filename][0] != src:
                raise ValueError(f"filename collision: {filename}")
            jobs[filename] = (src, v.get("digest"))
            v["urls"] = [filename]

    stats = {"cached": 0, "downloaded": 0}
    with cf.ThreadPoolExecutor(WORKERS) as pool:
        futures = {
            pool.submit(download, src, OUT / fn, digest): fn for fn, (src, digest) in jobs.items()
        }
        for fut in cf.as_completed(futures):
            stats[fut.result()] += 1

    for p in OUT.glob("*.tgz"):
        if p.name not in jobs:
            log.info("remove stale %s", p.name)
            p.unlink()
    for p in OUT.glob("*.part"):
        p.unlink()

    index["entries"] = entries
    (OUT / "index.yaml").write_text(
        yaml.safe_dump(index, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )

    size_mb = sum(p.stat().st_size for p in OUT.iterdir() if p.is_file()) / 1024 / 1024
    log.info(
        "charts=%d versions=%d downloaded=%d cached=%d size=%.1fMB",
        len(entries), len(jobs), stats["downloaded"], stats["cached"], size_mb,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
