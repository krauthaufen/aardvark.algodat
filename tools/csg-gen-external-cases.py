#!/usr/bin/env python3
"""Builds boolean test cases from external real-world meshes (CGAL data
collection, libigl tutorial data), with Manifold as validity filter and
ground-truth oracle.

Per valid mesh (normalized to unit scale), the pairing matrix covers the nasty
configurations: generic pose, exact self-copy (full coplanar coverage),
near-identical tiny rotation (grazing planes everywhere), epsilon-scale
translation, box/sphere carves, cross-model pairs and extreme-scale variants.

Data is cached in ~/.cache/csg-testdata. Output (not committed — external data
licenses): src/Aardvark.Algodat.Tests/PolyMesh/external-cases.json.gz,
consumed by CsgManifoldCases.ReplayExternalCases (skips when absent).
"""
import gzip
import json
import math
import os
import sys
import urllib.request

import numpy as np
import manifold3d as m3

CACHE = os.path.expanduser("~/.cache/csg-testdata")
OUT = os.path.join(
    os.path.dirname(__file__),
    "../src/Aardvark.Algodat.Tests/PolyMesh/external-cases.json.gz")

CGAL = "https://raw.githubusercontent.com/CGAL/cgal/master/Data/data/meshes/"
LIBIGL = "https://raw.githubusercontent.com/libigl/libigl-tutorial-data/master/"

SOURCES = [
    (CGAL, ["3torus.off", "anchor.off", "am.off", "blobby.off", "blobby_3cc.off",
            "cactus.off", "couplingdown.off", "cross.off", "elephant.off",
            "spool.off", "pig.off", "rotor.off", "handle.off", "joint.off",
            "mushroom.off", "oni.off", "seamount.off", "sphere966.off",
            "knot1.off", "knot2.off", "star.off", "tet-shuffled.off"]),
    (LIBIGL, ["cheburashka.off", "cow.off", "decimated-knight.off",
              "screwdriver.off", "fertility.off", "bumpy.off", "fandisk.off"]),
]

MAX_TRIS = 45000


def fetch(base, name):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, name)
    if not os.path.exists(path):
        try:
            urllib.request.urlretrieve(base + name, path)
        except Exception as e:
            print(f"  fetch {name}: {e}")
            return None
    return path


def load_off(path):
    with open(path, "r", errors="replace") as f:
        tokens = []
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                tokens.extend(line.split())
    if not tokens or tokens[0] not in ("OFF",):
        return None
    nv, nf = int(tokens[1]), int(tokens[2])
    at = 4
    verts = np.array(tokens[at:at + nv * 3], dtype=np.float64).reshape(nv, 3)
    at += nv * 3
    tris = []
    for _ in range(nf):
        k = int(tokens[at])
        idx = [int(x) for x in tokens[at + 1:at + 1 + k]]
        at += 1 + k
        for i in range(1, k - 1):  # fan; non-convex polygons fail validity later
            tris.append((idx[0], idx[i], idx[i + 1]))
    return verts, np.array(tris, dtype=np.int64)


def normalized(verts):
    c = 0.5 * (verts.min(axis=0) + verts.max(axis=0))
    s = np.abs(verts - c).max()
    return (verts - c) / max(s, 1e-30)


def build(verts, tris):
    try:
        m = m3.Manifold(m3.Mesh64(vert_properties=verts, tri_verts=tris.astype(np.uint64)))
    except Exception:
        return None
    if m.status() != m3.Error.NoError or m.num_tri() == 0:
        return None
    if m.volume() <= 0:
        return None
    return m


def mesh_json(m):
    mesh = m.to_mesh64()
    verts = np.asarray(mesh.vert_properties, dtype=np.float64).reshape(-1, 3)
    tris = np.asarray(mesh.tri_verts, dtype=np.int64).reshape(-1, 3)
    return {"vertices": [float(x) for x in verts.ravel()],
            "triangles": [int(x) for x in tris.ravel()]}


def rot(m, deg):
    return m.rotate((deg[0], deg[1], deg[2]))


cases = []


def add(name, a, b):
    if a.num_tri() + b.num_tri() > 2 * MAX_TRIS:
        return
    uv, ua = (a + b).volume(), (a + b).surface_area()
    iv, ia = (a ^ b).volume(), (a ^ b).surface_area()
    dv, da = (a - b).volume(), (a - b).surface_area()
    cases.append({
        "name": name, "a": mesh_json(a), "b": mesh_json(b),
        "unionVolume": uv, "interVolume": iv, "diffVolume": dv,
        "unionArea": ua, "interArea": ia, "diffArea": da,
    })
    print(f"  {name}: {a.num_tri()}+{b.num_tri()} tris")


solids = []
for base, names in SOURCES:
    for name in names:
        path = fetch(base, name)
        if path is None:
            continue
        loaded = load_off(path)
        if loaded is None:
            print(f"  {name}: unsupported format")
            continue
        verts, tris = loaded
        if len(tris) > MAX_TRIS:
            print(f"  {name}: too large ({len(tris)} tris)")
            continue
        m = build(normalized(verts), tris)
        if m is None:
            print(f"  {name}: rejected by manifold (open/non-manifold/inverted)")
            continue
        solids.append((name.rsplit(".", 1)[0], m))
        print(f"OK {name}: {m.num_tri()} tris, volume {m.volume():.4f}")

box = m3.Manifold.cube((1.2, 1.2, 1.2), True).translate((0.3, 0.2, 0.25))
ball = m3.Manifold.sphere(0.7, 48)

for name, m in solids:
    add(f"{name}-generic", m, rot(m, (17.8, 42.1, 12.0)).translate((0.2, 0.15, 0.1)))
    add(f"{name}-self", m, m)                                          # full coplanar coverage
    add(f"{name}-graze", m, rot(m, (6e-6, 0, 0)).translate((1e-7, 0, 0)))  # near-identical nightmare
    add(f"{name}-epsshift", m, m.translate((1e-7, 1e-7, 0)))
    add(f"{name}-box", m, box)
    add(f"{name}-ball", m, ball)

for i in range(len(solids) - 1):
    na, a = solids[i]
    nb, b = solids[i + 1]
    add(f"{na}-x-{nb}", a, b.translate((0.4, 0.2, 0.1)))

if len(solids) >= 2:
    for name, m in solids[:2]:
        big = m.scale((1e5, 1e5, 1e5)).translate((3e6, -2e6, 1e6))
        add(f"{name}-bigscale", big, rot(big, (17.8, 42.1, 12.0)))
        small = m.scale((1e-5, 1e-5, 1e-5))
        add(f"{name}-smallscale", small, small.translate((3e-6, 2e-6, 1e-6)))

os.makedirs(os.path.dirname(OUT), exist_ok=True)
with gzip.open(OUT, "wt") as f:
    json.dump({"cases": cases}, f)
print(f"wrote {len(cases)} cases to {os.path.abspath(OUT)} "
      f"({os.path.getsize(OUT) / 1e6:.1f} MB)")
