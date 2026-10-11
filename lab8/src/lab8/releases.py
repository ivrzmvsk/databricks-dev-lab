"""Scope probabilistic Gold repeatability checks to the same build inputs."""
from hashlib import sha256


def gold_build_fingerprint(root):
    paths = [root / 'lab8/assets/wikimedia.json',
             root / 'lab8/assets/access_policy.json']
    paths += list((root / 'lab6/src').rglob('*.py'))
    paths += list((root / 'lab8/src').rglob('*.py'))
    digest = sha256()
    for path in sorted(paths):
        name = path.relative_to(root).as_posix().encode()
        content = path.read_bytes()
        digest.update(len(name).to_bytes(8, 'big') + name)
        digest.update(len(content).to_bytes(8, 'big') + content)
    return digest.hexdigest()


def check_repeatability(releases, version, fingerprint, count, checksum):
    for release in releases:
        if release.source_version == version and release.build_fingerprint == fingerprint:
            if release.fact_count != count or release.fact_checksum != checksum:
                raise ValueError('Gold changed for the same Silver version and build inputs')
