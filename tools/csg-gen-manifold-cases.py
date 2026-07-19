#!/usr/bin/env python3
"""Generates hard boolean test cases with the Manifold library, recording
Manifold's own results as ground truth, for replay through
Aardvark.PolyMesh.Csg (CsgManifoldCases test).

The shapes mirror Manifold's test-suite stress cases: coaxial/crossing/tangent
cylinders, high-res and near-tangent spheres, twisted extrusions, tori,
gyroid level sets, warped spheres, sponge-like rod subtractions, and
epsilon-offset cubes producing sliver results.

Output: gzipped JSON at src/Aardvark.Algodat.Tests/PolyMesh/manifold-cases.json.gz
"""
import gzip
import json
import math
import os

import numpy as np
import manifold3d as m3


def mesh_of(m):
    mesh = m.to_mesh64()
    verts = np.asarray(mesh.vert_properties, dtype=np.float64).reshape(-1, 3)
    tris = np.asarray(mesh.tri_verts, dtype=np.int64).reshape(-1, 3)
    return {
        "vertices": [float(x) for x in verts.ravel()],
        "triangles": [int(x) for x in tris.ravel()],
    }


def va(m):
    return float(m.volume()), float(m.surface_area())


cases = []


def add(name, a, b):
    uv, ua = va(a + b)
    iv, ia = va(a ^ b)
    dv, da = va(a - b)
    cases.append(
        {
            "name": name,
            "a": mesh_of(a),
            "b": mesh_of(b),
            "unionVolume": uv, "interVolume": iv, "diffVolume": dv,
            "unionArea": ua, "interArea": ia, "diffArea": da,
        }
    )
    print(f"{name}: {a.num_tri()}+{b.num_tri()} tris, union vol {uv:.6g}")


# --- cylinders ---------------------------------------------------------------
cyl = m3.Manifold.cylinder(2.0, 1.0, circular_segments=48, center=True)
add("cyl-coaxial", cyl, m3.Manifold.cylinder(3.0, 0.5, circular_segments=48, center=True))
add("cyl-cross", cyl, cyl.rotate((90, 0, 0)))
add("cyl-cross-shift", cyl, cyl.rotate((90, 0, 0)).translate((0.3, 0.2, 0.1)))
add("cyl-tangent", cyl, cyl.translate((2.0, 0, 0)))  # side tangency, exact
add("cyl-stack", cyl, cyl.translate((0, 0, 2.0)))    # coplanar cap contact

# --- spheres -----------------------------------------------------------------
sph = m3.Manifold.sphere(1.0, 64)
add("sphere-hi", m3.Manifold.sphere(1.0, 128), m3.Manifold.sphere(1.0, 128).translate((0.5, 0.3, 0.2)))
add("sphere-near-tangent", sph, sph.translate((1.999999, 0, 0)))
add("sphere-tiny-overlap", sph, sph.translate((1.9, 0, 0)))
add("sphere-concentric", sph, m3.Manifold.sphere(0.5, 32))
add("sphere-in-cube", m3.Manifold.cube((2, 2, 2), True), sph)

# --- tetrahedra --------------------------------------------------------------
tet = m3.Manifold.tetrahedron()
add("tetra-overlap", tet, tet.rotate((0, 0, 90)).translate((0.5, 0.2, 0.3)))
add("tetra-touch", tet, tet.translate((2.0, 2.0, 2.0)))  # vertex contact at (1,1,1)

# --- extrusions / revolve ----------------------------------------------------
star = m3.CrossSection(
    [[(math.cos(a) * (1.0 if i % 2 == 0 else 0.45),
       math.sin(a) * (1.0 if i % 2 == 0 else 0.45))
      for i, a in enumerate(np.linspace(0, 2 * math.pi, 10, endpoint=False))]]
)
twisted = m3.Manifold.extrude(star, 2.0, n_divisions=32, twist_degrees=120.0)
add("twist-box", twisted, m3.Manifold.cube((1.5, 1.5, 1.0)).translate((-0.75, -0.75, 0.5)))
add("twist-twist", twisted, twisted.rotate((0, 0, 36)).translate((0.3, 0, 0.4)))

circle = m3.CrossSection([[(2.0 + math.cos(a) * 0.6, math.sin(a) * 0.6)
                           for a in np.linspace(0, 2 * math.pi, 32, endpoint=False)]])
torus = m3.Manifold.revolve(circle, circular_segments=48)
add("torus-cyl", torus, m3.Manifold.cylinder(4.0, 1.9, circular_segments=48, center=True))
add("torus-torus", torus, torus.rotate((90, 0, 0)))

# --- gyroid (level set) ------------------------------------------------------
def gyroid(x, y, z):
    return (math.sin(x) * math.cos(y) + math.sin(y) * math.cos(z)
            + math.sin(z) * math.cos(x) + 0.4)

gy = m3.Manifold.level_set(gyroid, [-3.0, -3.0, -3.0, 3.0, 3.0, 3.0], 0.35)
add("gyroid-cube", gy, m3.Manifold.cube((4, 4, 4), True))
add("gyroid-gyroid", gy, gy.translate((0.8, 0.4, 0.2)))

# --- warped spheres ----------------------------------------------------------
def bump(p):
    x, y, z = p
    s = 1.0 + 0.15 * math.sin(5 * x) * math.sin(5 * y) * math.sin(5 * z)
    return (x * s, y * s, z * s)

wob = m3.Manifold.sphere(1.0, 64).warp(bump)
add("warp-warp", wob, wob.rotate((0, 90, 0)).translate((0.4, 0.2, 0.1)))

# --- sponge-like rod subtraction --------------------------------------------
rodx = m3.Manifold.cube((4, 0.6, 0.6), True)
rods = rodx + rodx.rotate((0, 0, 90)) + rodx.rotate((0, 90, 0))
sponge = m3.Manifold.cube((3, 3, 3), True) - rods
add("sponge-sponge", sponge, sponge.rotate((0, 0, 45)).translate((0.5, 0.3, 1.0)))
add("sponge-sphere", sponge, m3.Manifold.sphere(1.4, 48))

# --- epsilon slivers ---------------------------------------------------------
cube = m3.Manifold.cube((1, 1, 1))
add("cube-eps-sliver", cube, cube.translate((1e-7, 0, 0)))
add("cube-eps-rot", cube, cube.rotate((0, 0, 1e-5)).translate((0.5, 0.5, 0)))

out = os.path.join(
    os.path.dirname(__file__),
    "../src/Aardvark.Algodat.Tests/PolyMesh/manifold-cases.json.gz",
)
with gzip.open(out, "wt") as f:
    json.dump({"cases": cases}, f)
print(f"wrote {len(cases)} cases to {os.path.abspath(out)} "
      f"({os.path.getsize(out) / 1e6:.1f} MB)")
