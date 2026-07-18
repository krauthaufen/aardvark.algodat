# Aardvark.PolyMesh.Csg — Design

Reliable mesh booleans (union / intersection / difference / xor) for `PolyMesh`,
pure managed .NET, built on Aardvark.Base types.

## Goals

- **Correctness first.** Ternary eps-classification (`Below | On | Above`) is a
  first-class state at *every* decision site. Coplanar faces, edge-through-vertex,
  vertex-on-face, shared geometry — designed-for cases, not epsilon accidents.
- **Scale/offset invariance.** All tolerances are relative to the magnitudes that
  enter a computation. The same model at scale 1e-3 or 1e6, near origin or offset
  by 1e7, classifies identically. No quantization/discretization of any kind.
- **Guaranteed manifold output** for watertight manifold input — enforced by
  construction and checked by a verifier that throws rather than returning garbage.
- **SoA internals, custom attribute interpolation** via PolyMesh's `Attribute<T>`
  interpolator infrastructure (`Func<double,T,T,T>` per channel, user-extensible).
- Deterministic: same inputs → bit-identical output (v0 single-threaded kernel).

## Non-goals (v0)

- Non-watertight / non-manifold input repair (later, separate layer).
- Polygonal output (kernel emits triangles; coplanar re-merging via topology later).
- Self-intersecting inputs.
- Parallelism (design leaves room: per-face-pair work is independent; the
  classification cache is append-only).

## Input / output contract

Input: two `PolyMesh` instances, each a watertight orientable 2-manifold
(consistent winding, every edge shared by exactly 2 faces). Verified on ingest
(cheap, via `BuildTopology` counts) — violations throw `CsgInputException`.

Faces may be arbitrary planar polygons; ingest triangulates, but **all triangles
of one source polygon share that polygon's single source plane** (see Planes).

Output: triangle `PolyMesh` with interpolated attributes, verified manifold.
Failure to verify throws `CsgVerificationException` — never a silent bad mesh.

## The eps model

### Relative tolerance

One user-facing knob: `RelativeEpsilon` (default 1e-11: ~4 orders of magnitude
above accumulated double rounding noise, while still preserving micrometer
detail at 10 km offsets).

Point–plane test for vertex `v` against unit-normal plane `(n, d)`:

```
h   = n·v + d                       // signed distance
tol = eps · (|v.X|+|v.Y|+|v.Z| + |d|)   // scales with the magnitudes summed in h
sign = h < -tol ? Below : h > tol ? Above : On
```

The tol formula mirrors the floating-point error bound of evaluating `h` itself
(error of a dot product is proportional to Σ|terms|), so tolerance and actual
fp uncertainty scale together — this is what makes the model offset-invariant:
at offset 1e7 the representable grid is coarser and tol grows with it.

All tolerances additionally include a **scene term** `eps · Scene` (Scene =
max |coordinate| over all input vertices): a plane or point is only known to
within the slop of the geometry that defined it, so tolerances must not
collapse for points near the origin, where the local magnitude terms vanish
(found by fuzzing: a vertex at the origin classified strictly Below a plane it
was 1e-16 away from, because `eps·(|p|+|d|)` was ~1e-27 there). The scene term
keeps every predicate exactly invariant under whole-scene transforms.

Vertex–vertex coincidence: `|a-b|∞ ≤ eps · (|a|∞ + |b|∞ + Scene)`.
2D orientation (ear clipping, in-plane booleans): `tol = eps · (m + L) · L` with
m = max coordinate magnitude and L = max edge extent — the propagation of
per-coordinate slop eps·m through the determinant; never eps·m², which would
swallow everything far from the origin.
Plane–plane coincidence: unit normals within angular eps AND each plane's
offset within the point–plane tol of the other (tested via sample points on
the actual face, not just |d| — offsets far from a face's support are meaningless).

### Error propagation (derived points)

Cut points (edge×plane, segment×segment in-plane) are computed, not input, and
carry one extra generation of rounding error. Every kernel vertex has a small
integer **tolerance generation** g (input = 0, cut = 1); classification of a
generation-g point uses `tol · C^g` with a fixed conservative constant C (≈ 8).
The two-mesh boolean needs no generation ≥ 2: all cuts are computed against
canonical planes from the *original* inputs, never against derived geometry.

### Consistency despite non-transitivity

Eps-equality is not transitive; naive independent predicates eventually
contradict each other, and contradictions become cracks. Three structural rules:

1. **Canonicalize first, decide later.** Before any topology decision:
   - *Plane welding:* cluster near-coincident planes (both meshes) via
     union-find → canonical plane per cluster. Triangles from the same source
     polygon share a plane id by construction, so intra-polygon coplanarity is
     exact, not eps.
   - *Vertex welding:* cluster near-coincident vertices across both meshes →
     canonical vertex per cluster (position = representative, not average, so
     welding is idempotent). Shared-face booleans (`A ∪ A`) reduce to exact
     index equality after this.
2. **Every predicate computed once.** Classification is a memoized function
   `(canonicalVertexId, canonicalPlaneId) → Sign3`, stored in a cache. No call
   site ever re-derives it differently. Ditto cut points: memoized by
   `(canonicalEdgeKey, canonicalPlaneId)` so both faces sharing an edge, and
   both meshes, see the *identical* cut vertex (id, position, and attributes).
3. **`On` commits.** A vertex classified `On` a plane is treated as exactly on
   it everywhere downstream (its height is *conceptually zero*, never "small").
   A face whose three vertices are `On` a canonical plane *is* coplanar and is
   routed to the 2D coplanar pipeline — there is no "almost coplanar" limbo.

Planes are ground truth: face planes are computed once at ingest (Newell for
polygons, exact cross for triangles), canonicalized, and never re-fit from
positions mid-algorithm.

## Kernel representation (SoA)

Immutable input side, append-only working side. All flat arrays, int indices:

- `V3d[] positions` (append-only: input verts of A, input verts of B, cut verts)
- `byte[] generation` (tolerance generation per vertex)
- `int[] triV0/V1/V2`, `int[] triPlane` (canonical plane id), `int[] triSource`
  (source mesh + source face id, for attributes and inside/outside bookkeeping)
- `Plane3d[] planes` (canonical, unit normals)
- Classification cache: per (vertex, plane) `Sign3` — plane-major
  `Dictionary<long,(sbyte)>` keyed `planeId<<32|vertexId` in v0; revisit layout
  when profiling.
- Attribute channels ride along as parallel arrays; cut vertices store
  provenance `(sourceFace, barycentric)` so any channel can be produced lazily
  through its interpolator (`Attribute<T>.Interpolator`, normalizing lerp for
  normals, step for discrete types — reused from PolyMesh).

`Sign3` is `enum : sbyte { Below = -1, On = 0, Above = 1 }`. Every switch over
it handles all three members explicitly; a `default:` arm throws.

## Pipeline

```
ingest A, ingest B          PolyMesh → kernel SoA; triangulate polygons (keep
                            source-polygon plane); verify watertight manifold
canonicalize                weld planes, weld vertices (union-find), build caches
broad phase                 BVH over triangles of A vs B → candidate pairs
arrangement                 per candidate pair, classify + cut:
                              non-coplanar pair → intersection segment via the
                                shared classification cache & memoized cut points
                              coplanar pair (same canonical plane) → defer to
                                2D coplanar pipeline
subdivision                 per original triangle: constrained triangulation
                            (in its plane's dominant-axis 2D projection) of the
                            triangle + its collected segments/on-vertices
coplanar pipeline           per shared plane: 2D boolean of the two facet sets
                            (same ternary machinery in 2D: line sides Below/On/
                            Above); output facets tagged same/opposite winding
classification              each output fragment is inside/outside/on-boundary
                            w.r.t. the other solid: seed fragments by robust
                            point-in-solid (ray cast with ternary hit filtering,
                            re-cast on any On-grade hit), then flood-fill across
                            edges that are not on the intersection curve —
                            O(#fragments) point tests avoided, consistency
                            inherited from connectivity
boolean selection           union/intersection/difference/xor = per-fragment
                            keep/discard/flip table over (side, boundary-tag)
stitch + emit               build output halfedges; cut vertices already shared
                            by id ⇒ boundary closes by construction; interpolate
                            attribute channels; emit PolyMesh
verify                      every edge degree 2, consistent orientation,
                            components closed, Euler check, no degenerate
                            triangles at output tolerance → else throw
```

### The On-cases (must-pass, not best-effort)

- vertex of A on a face/edge/vertex of B (all pairs)
- edge of A through a vertex of B; edge-on-edge (collinear overlap)
- face-on-face same winding, opposite winding, partial overlap
- `A op A`, `A op translate(A, eps·scale/2)`, `A op translate(A, 2·bbox)`
- tangent contact (sphere touching plane at one On-vertex)

Each is a designed path: the arrangement consults the ternary cache, so e.g. an
edge whose endpoint is `On` a plane produces a cut *at that vertex* (no new
point, no sliver), and collinear edge overlaps produce shared sub-edges.

## API sketch

```csharp
public static class Csg
{
    // one PolyMesh per edge-connected output component: multi-part results
    // (union of disjoint solids, xor) come back as separate clean solids, and
    // solids touching only at a vertex stay two components instead of sharing
    // a non-manifold vertex
    public static PolyMesh[] Union(PolyMesh a, PolyMesh b, CsgOptions? o = null);
    public static PolyMesh[] Intersection(PolyMesh a, PolyMesh b, CsgOptions? o = null);
    public static PolyMesh[] Difference(PolyMesh a, PolyMesh b, CsgOptions? o = null);
    public static PolyMesh[] Xor(PolyMesh a, PolyMesh b, CsgOptions? o = null);
    // one arrangement, four selections:
    public static CsgArrangement Arrange(PolyMesh a, PolyMesh b, CsgOptions? o = null);
}

public sealed class CsgOptions
{
    public double RelativeEpsilon = 1e-11;
    public CsgVerification Verification = CsgVerification.Full; // Full | InputOnly | None
    public SymbolDict<object>? AttributeInterpolators;          // per-channel overrides
}
```

`CsgArrangement` holds the arranged fragments + classifications; `.Union()` etc.
are cheap selections — the natural API for CSG trees later.

## Reuse from the platform (and explicit non-reuse)

Reused: `PolyMesh` as I/O format; `Attribute<T>`/`IAttribute` interpolators;
`BuildTopology` counts for input verification; `TriangulatedCopy`'s
triangulator *algorithm* as reference for ingest (tested, trusted).

Not depended on (unreliable or wrong eps model): `SplitOnPlane` (absolute eps),
`ClipByPlane` (documented broken caps/attributes), `VertexClusteredCopy`,
face clustering, `Contains`. `PolygonSplitter` is design inspiration
(ternary split, shared cut vertices) but the kernel has its own code.

## Testing strategy

- **On-case matrix** above, each × {scale 1e-3, 1, 1e6} × {offset 0, 1e7} —
  the same test body asserts identical topology across all scale/offset
  variants (this pins scale invariance as a property, not a hope).
- **Property tests** on primitive pairs (boxes, spheres, cylinders at random
  poses): output verifies manifold; volume(A∪B)+volume(A∩B) =
  volume(A)+volume(B) within tolerance; `A∖B` + `A∩B` = `A`;
  union-with-self idempotent; op(A,B) ≍ op(B,A) (same volume/topology).
- **Attribute tests**: interpolated normals unit-length; custom channel with a
  bespoke interpolator survives the boolean; step-channels never blend.
- **Adversarial fixtures**: near-tangent spheres, grazing boxes rotated by
  eps-scale angles, needle triangles from thin boxes.
- NUnit in `Aardvark.Algodat.Tests` (net8.0), fixtures built from
  `PolyMeshPrimitives` + hand-constructed meshes (primitives are simple enough
  to trust after a one-time manifold check in the fixture builder).

## Milestones

- **M0** Scaffold; ingest (triangulation, planes, verification); verifier;
  identity pipeline (`Arrange` with no intersections → disjoint union works).
- **M1** Canonicalization + classification cache + BVH broad phase.
- **M2** Generic-position booleans (no On-states between the meshes): tri–tri
  segments, subdivision, seed+flood classification, stitch, verify. Property
  tests green for boxes/spheres in general position.
- **M3** On-cases: vertex/edge/face contact matrix green at all scales/offsets.
- **M4** Coplanar 2D boolean pipeline; shared-face matrix green.
- **M5** Perf pass: SoA layout tuning, cache layout, `Parallel.For` over face
  pairs, benchmarks vs Manifold (via its CLI, as external oracle) on primitive
  fleets.
