#!/usr/bin/env python3
"""Differential test of Aardvark.PolyMesh.Csg against the Manifold library.

Reads the JSON produced by the CsgDifferential.Export NUnit test (inputs plus
our boolean volumes/areas), runs the same booleans through manifold3d, and
compares. Volume is compared tightly; area a bit more loosely (measure-zero
contact conventions may legitimately differ between the two libraries).

Usage: csg-diff.py [cases.json]   (default /tmp/csgdiff-cases.json)
"""
import json
import sys

import numpy as np
import manifold3d as m3

VOL_REL = 1e-8
AREA_REL = 1e-6


def build(mesh):
    verts = np.asarray(mesh["vertices"], dtype=np.float64).reshape(-1, 3)
    tris = np.asarray(mesh["triangles"], dtype=np.uint64).reshape(-1, 3)
    try:
        m = m3.Manifold(m3.Mesh64(vert_properties=verts, tri_verts=tris))
    except (AttributeError, TypeError):
        m = m3.Manifold(
            m3.Mesh(
                vert_properties=verts.astype(np.float32),
                tri_verts=tris.astype(np.uint32),
            )
        )
    if m.status() != m3.Error.NoError:
        raise RuntimeError(f"manifold rejected input: {m.status()}")
    return m


def vol_area(m):
    v = m.volume() if callable(getattr(m, "volume")) else m.volume
    a = m.surface_area() if callable(getattr(m, "surface_area")) else m.surface_area
    return float(v), float(a)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/csgdiff-cases.json"
    with open(path) as f:
        cases = json.load(f)["cases"]

    failures = []
    ours_failed = []
    for case in cases:
        name = case["name"]
        if "failed" in case:
            ours_failed.append((name, case["failed"]))
            continue
        try:
            a = build(case["a"])
            b = build(case["b"])
        except RuntimeError as e:
            failures.append((name, f"input rejected by manifold: {e}"))
            continue

        scale = abs(vol_area(a)[0]) + abs(vol_area(b)[0])
        coplanar = case.get("coplanarContact", False)
        for op, mf in (("union", a + b), ("inter", a ^ b), ("diff", a - b)):
            mv, ma = vol_area(mf)
            ov = case[f"{op}Volume"]
            oa = case[f"{op}Area"]
            if abs(mv - ov) > VOL_REL * scale:
                failures.append((name, f"{op} volume: ours {ov!r} manifold {mv!r}"))
            # at coincident-surface contact the two libraries legitimately
            # differ in which measure-zero sheets they keep: skip area there
            if not coplanar and abs(ma - oa) > AREA_REL * max(1.0, ma, oa):
                failures.append((name, f"{op} area: ours {oa!r} manifold {ma!r}"))

    for name, msg in ours_failed:
        print(f"OURS-FAILED {name}: {msg}")
    for name, msg in failures:
        print(f"MISMATCH {name}: {msg}")
    print(
        f"csg-diff: {len(cases)} cases, {len(failures)} mismatches, "
        f"{len(ours_failed)} failures on our side"
    )
    sys.exit(1 if failures or ours_failed else 0)


if __name__ == "__main__":
    main()
