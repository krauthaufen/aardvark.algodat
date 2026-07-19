using System;
using System.Collections.Generic;
using System.Linq;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>Knobs for <see cref="PolyMeshRepair"/>.</summary>
    public sealed class RepairOptions
    {
        /// <summary>
        /// Absolute weld tolerance; vertices closer than this merge. 0 means
        /// derive it from the bounding box (RelativeWeldTolerance × diagonal).
        /// </summary>
        public double WeldTolerance { get; init; } = 0.0;

        /// <summary>Weld tolerance as a fraction of the bounding-box diagonal when WeldTolerance is 0.</summary>
        public double RelativeWeldTolerance { get; init; } = 1e-7;

        /// <summary>Triangulate boundary loops shut so each component is closed.</summary>
        public bool CloseHoles { get; init; } = true;

        /// <summary>
        /// Only close a boundary loop whose span (bounding-box diagonal of its
        /// vertices) is at most this fraction of the whole mesh's diagonal — a
        /// larger loop is treated as a deliberate opening and left open (that
        /// component is then dropped, since it is not a closed solid). Infinity
        /// closes every loop.
        /// </summary>
        public double MaxHoleSpanFraction { get; init; } = double.PositiveInfinity;

        public static readonly RepairOptions Default = new();
    }

    /// <summary>
    /// Turns an arbitrary triangle soup / indexed mesh into zero or more
    /// watertight orientable 2-manifolds fit for the CSG kernel. Handles the
    /// defects real meshes carry: near-coincident (unwelded) vertices,
    /// degenerate and duplicated faces, coincident opposite-faces, inconsistent
    /// winding, small holes, and non-manifold edges/vertices where several
    /// sheets meet (those are separated into distinct manifolds). Purely a
    /// topological-cleanup + hole-fill pass; it does not resolve
    /// self-intersections (that is the CSG kernel run against the mesh itself).
    ///
    /// Every returned PolyMesh passes <see cref="ManifoldChecks"/>.
    /// </summary>
    public static class PolyMeshRepair
    {
        public static PolyMesh[] Repair(PolyMesh mesh, RepairOptions? options = null)
        {
            var o = options ?? RepairOptions.Default;
            var positions = mesh.PositionArray;
            if (positions == null || positions.Length == 0) return Array.Empty<PolyMesh>();

            // 1. fan-triangulate every input face into a triangle soup
            var tris = new List<int>();
            {
                var fia = mesh.FirstIndexArray; var via = mesh.VertexIndexArray;
                for (var fi = 0; fi + 1 < fia.Length; fi++)
                {
                    var s = fia[fi]; var e = fia[fi + 1];
                    for (var i = s + 1; i + 1 < e; i++)
                    {
                        tris.Add(via[s]); tris.Add(via[i]); tris.Add(via[i + 1]);
                    }
                }
            }
            if (tris.Count == 0) return Array.Empty<PolyMesh>();

            // 2. weld near-coincident vertices (union-find over a spatial hash)
            var bounds = new Box3d(positions);
            var diag = bounds.Size.Length;
            var tol = o.WeldTolerance > 0 ? o.WeldTolerance : o.RelativeWeldTolerance * diag.Max(1e-300);
            var rep = WeldVertices(positions, tol);
            for (var i = 0; i < tris.Count; i++) tris[i] = rep[tris[i]];

            // 3. drop collapsed triangles, then cancel/dedup coincident faces
            //    (same three vertices): opposite winding = zero-volume flap →
            //    drop both; equal winding = duplicate → keep one
            var cleaned = CancelCoincident(DropCollapsed(tris));
            if (cleaned.Count == 0) return Array.Empty<PolyMesh>();

            // 4. make the winding consistent across manifold (2-face) edges
            OrientConsistently(cleaned);

            // 5. close holes so each surface is watertight, then split into
            //    manifold components (separating non-manifold edges/vertices)
            if (o.CloseHoles) CloseHoles(cleaned, positions, diag * SpanCap(o));
            var components = SplitManifolds(cleaned, positions);

            // 6. compact + orient outward + verify each component
            var result = new List<PolyMesh>(components.Count);
            foreach (var comp in components)
            {
                var pm = BuildComponent(comp, positions);
                if (pm == null) continue; // degenerate/empty after compaction
                result.Add(pm);
            }
            return result.ToArray();
        }

        private static double SpanCap(RepairOptions o)
            => double.IsInfinity(o.MaxHoleSpanFraction) ? double.PositiveInfinity : o.MaxHoleSpanFraction;

        #region welding

        private static int[] WeldVertices(V3d[] positions, double tol)
        {
            var n = positions.Length;
            var parent = new int[n].SetByIndex(i => i);
            int Find(int i) { while (parent[i] != i) { parent[i] = parent[parent[i]]; i = parent[i]; } return i; }
            void Union(int a, int b) { var ra = Find(a); var rb = Find(b); if (ra != rb) parent[ra.Max(rb)] = ra.Min(rb); }

            if (tol > 0)
            {
                var h = tol;
                var grid = new Dictionary<(long, long, long), List<int>>();
                (long, long, long) Cell(V3d p) => ((long)Fun.Floor(p.X / h), (long)Fun.Floor(p.Y / h), (long)Fun.Floor(p.Z / h));
                for (var i = 0; i < n; i++)
                {
                    var p = positions[i];
                    var c = Cell(p);
                    for (var dx = -1; dx <= 1; dx++)
                        for (var dy = -1; dy <= 1; dy++)
                            for (var dz = -1; dz <= 1; dz++)
                            {
                                if (!grid.TryGetValue((c.Item1 + dx, c.Item2 + dy, c.Item3 + dz), out var bucket)) continue;
                                foreach (var j in bucket)
                                    if ((positions[j] - p).Length <= tol) Union(i, j);
                            }
                    grid.GetOrCreate(c, _ => new List<int>()).Add(i);
                }
            }
            var result = new int[n];
            for (var i = 0; i < n; i++) result[i] = Find(i);
            return result;
        }

        #endregion

        #region face cleanup

        private static List<int> DropCollapsed(List<int> tris)
        {
            var outT = new List<int>(tris.Count);
            for (var t = 0; t < tris.Count; t += 3)
            {
                var a = tris[t]; var b = tris[t + 1]; var c = tris[t + 2];
                if (a == b || b == c || c == a) continue;
                outT.Add(a); outT.Add(b); outT.Add(c);
            }
            return outT;
        }

        /// <summary>Reduces each group of same-vertex-triple triangles to one net-orientation representative (or none).</summary>
        private static List<int> CancelCoincident(List<int> tris)
        {
            var net = new Dictionary<(int, int, int), int>();
            for (var t = 0; t < tris.Count; t += 3)
            {
                var key = Sorted(tris[t], tris[t + 1], tris[t + 2], out var positive);
                net[key] = (net.TryGetValue(key, out var c) ? c : 0) + (positive ? 1 : -1);
            }
            var outT = new List<int>(tris.Count);
            var emitted = new HashSet<(int, int, int)>();
            for (var t = 0; t < tris.Count; t += 3)
            {
                var key = Sorted(tris[t], tris[t + 1], tris[t + 2], out _);
                if (emitted.Contains(key)) continue;
                var nv = net[key];
                if (nv == 0) { emitted.Add(key); continue; }
                emitted.Add(key);
                // emit one triangle with the sorted-order winding of the net sign
                var (x, y, z) = key;
                if (nv > 0) { outT.Add(x); outT.Add(y); outT.Add(z); }
                else { outT.Add(x); outT.Add(z); outT.Add(y); }
            }
            return outT;
        }

        private static (int, int, int) Sorted(int a, int b, int c, out bool positive)
        {
            var swaps = 0;
            if (a > b) { (a, b) = (b, a); swaps++; }
            if (b > c) { (b, c) = (c, b); swaps++; }
            if (a > b) { (a, b) = (b, a); swaps++; }
            positive = (swaps & 1) == 0;
            return (a, b, c);
        }

        #endregion

        #region orientation

        /// <summary>
        /// Flips triangles so adjacent faces agree across shared manifold
        /// edges. Propagates only over edges with exactly two incident faces
        /// (a non-manifold edge has no canonical orientation to cross);
        /// non-orientable or ambiguous patches are left as-is.
        /// </summary>
        private static void OrientConsistently(List<int> tris)
        {
            var triCount = tris.Count / 3;
            // undirected edge -> incident halfedges
            var edges = new Dictionary<long, List<int>>();
            for (var h = 0; h < tris.Count; h++)
            {
                var t = h / 3; var k = h % 3;
                var u = tris[t * 3 + k]; var v = tris[t * 3 + (k + 1) % 3];
                edges.GetOrCreate(UKey(u, v), _ => new List<int>()).Add(h);
            }

            var oriented = new bool[triCount];
            var stack = new Stack<int>();
            for (var seed = 0; seed < triCount; seed++)
            {
                if (oriented[seed]) continue;
                oriented[seed] = true;
                stack.Push(seed);
                while (stack.Count > 0)
                {
                    var t = stack.Pop();
                    for (var k = 0; k < 3; k++)
                    {
                        var u = tris[t * 3 + k]; var v = tris[t * 3 + (k + 1) % 3];
                        var incident = edges[UKey(u, v)];
                        if (incident.Count != 2) continue; // only manifold edges carry orientation
                        var other = incident[0] / 3 == t ? incident[1] : incident[0];
                        var nt = other / 3;
                        if (oriented[nt]) continue;
                        // consistent iff nt traverses the shared edge as v->u
                        var ok = tris[other] == v && tris[other - other % 3 + (other % 3 + 1) % 3] == u;
                        if (!ok) (tris[nt * 3 + 1], tris[nt * 3 + 2]) = (tris[nt * 3 + 2], tris[nt * 3 + 1]);
                        oriented[nt] = true;
                        stack.Push(nt);
                    }
                }
            }
        }

        #endregion

        #region hole closing

        /// <summary>Triangulates boundary loops (fan over the reversed loop) so every edge gains its missing twin.</summary>
        private static void CloseHoles(List<int> tris, V3d[] positions, double maxSpan)
        {
            // boundary halfedge = a directed edge whose reverse is absent
            var dir = new HashSet<long>();
            for (var h = 0; h < tris.Count; h++)
            {
                var t = h / 3; var k = h % 3;
                dir.Add(DKey(tris[t * 3 + k], tris[t * 3 + (k + 1) % 3]));
            }
            // outgoing boundary edges per vertex
            var nextOf = new Dictionary<int, List<int>>();
            for (var h = 0; h < tris.Count; h++)
            {
                var t = h / 3; var k = h % 3;
                var a = tris[t * 3 + k]; var b = tris[t * 3 + (k + 1) % 3];
                if (!dir.Contains(DKey(b, a)))
                    nextOf.GetOrCreate(a, _ => new List<int>()).Add(b);
            }

            var used = new HashSet<long>();
            foreach (var (startA, outs) in nextOf.ToList())
            {
                foreach (var startB in outs)
                {
                    if (used.Contains(DKey(startA, startB))) continue;
                    // trace the loop
                    var loop = new List<int> { startA };
                    var a = startA; var b = startB;
                    var ok = true;
                    for (var guard = 0; ; guard++)
                    {
                        used.Add(DKey(a, b));
                        loop.Add(b);
                        if (b == startA) break;
                        if (!nextOf.TryGetValue(b, out var cand) || guard > tris.Count)
                        { ok = false; break; }
                        // pick an unused boundary edge out of b
                        var nb = -1;
                        foreach (var c in cand) if (!used.Contains(DKey(b, c))) { nb = c; break; }
                        if (nb < 0) { ok = false; break; }
                        a = b; b = nb;
                    }
                    if (!ok || loop.Count < 4) continue; // loop[last]==startA duplicate → need ≥3 distinct
                    loop.RemoveAt(loop.Count - 1); // drop the closing duplicate
                    if (loop.Count < 3) continue;

                    var span = new Box3d(loop.Select(v => positions[v])).Size.Length;
                    if (span > maxSpan) continue; // deliberate opening, leave it

                    // fan-triangulate the REVERSED loop so new edges are the
                    // missing twins (existing boundary is v0->v1->...; the patch
                    // supplies v1->v0, v2->v1, ...)
                    for (var i = loop.Count - 1; i >= 2; i--)
                    {
                        tris.Add(loop[0]); tris.Add(loop[i]); tris.Add(loop[i - 1]);
                    }
                }
            }
        }

        #endregion

        #region manifold splitting

        /// <summary>
        /// Pairs half-edges (angular ordering at edges where more than two
        /// faces meet), extracts edge-connected components through the pairing,
        /// and splits pinch vertices by grouping each vertex's incident corners
        /// by link-connectivity — so several sheets meeting at one edge or
        /// vertex become distinct manifolds. Returns per-component triangle
        /// lists in a local vertex numbering (kernel ids preserved via the
        /// component's own remap done in BuildComponent).
        /// </summary>
        private static List<List<int>> SplitManifolds(List<int> tris, V3d[] positions)
        {
            var h = tris.Count;
            var twin = PairHalfEdges(tris, positions);

            // components through paired half-edges
            var triCount = h / 3;
            var comp = new int[triCount].Set(-1);
            var count = 0;
            var stack = new Stack<int>();
            for (var seed = 0; seed < triCount; seed++)
            {
                if (comp[seed] >= 0) continue;
                var ci = count++;
                comp[seed] = ci; stack.Push(seed);
                while (stack.Count > 0)
                {
                    var t = stack.Pop();
                    for (var k = 0; k < 3; k++)
                    {
                        var tw = twin[t * 3 + k];
                        if (tw < 0) continue;
                        var nt = tw / 3;
                        if (comp[nt] < 0) { comp[nt] = ci; stack.Push(nt); }
                    }
                }
            }

            var groups = new List<List<int>>();
            for (var i = 0; i < count; i++) groups.Add(new List<int>());
            for (var t = 0; t < triCount; t++)
            {
                groups[comp[t]].Add(tris[t * 3]);
                groups[comp[t]].Add(tris[t * 3 + 1]);
                groups[comp[t]].Add(tris[t * 3 + 2]);
            }
            return groups;
        }

        /// <summary>twin[h] = the half-edge paired across h's undirected edge, or -1 (boundary).</summary>
        private static int[] PairHalfEdges(List<int> tris, V3d[] positions)
        {
            var h = tris.Count;
            var twin = new int[h].Set(-1);
            var edges = new Dictionary<long, List<int>>();
            for (var he = 0; he < h; he++)
            {
                var t = he / 3; var k = he % 3;
                edges.GetOrCreate(UKey(tris[t * 3 + k], tris[t * 3 + (k + 1) % 3]), _ => new List<int>()).Add(he);
            }
            foreach (var (key, hes) in edges)
            {
                if (hes.Count == 2) { twin[hes[0]] = hes[1]; twin[hes[1]] = hes[0]; continue; }
                if (hes.Count < 2) continue; // boundary (should be gone after hole close)
                PairAngular(tris, positions, key, hes, twin);
            }
            return twin;
        }

        /// <summary>
        /// Pairs a bundle of &gt;2 half-edges around one edge into solid wedges
        /// by dihedral angle: sorted around the edge axis, each face traversing
        /// v→u pairs with the next one traversing u→v. Keeps distinct sheets
        /// (e.g. two solids sharing an edge) from being cross-linked.
        /// </summary>
        private static void PairAngular(List<int> tris, V3d[] positions, long key, List<int> hes, int[] twin)
        {
            var u = (int)(key >> 32); var v = (int)key;
            var d = (positions[v] - positions[u]).Normalized;
            var ax0 = d.X.Abs() < 0.9 ? V3d.XAxis : V3d.YAxis;
            var ax1 = d.Cross(ax0).Normalized;
            var ax2 = d.Cross(ax1);
            var around = hes.Map(he =>
            {
                var t = he / 3;
                int a = tris[t * 3], b = tris[t * 3 + 1], c = tris[t * 3 + 2];
                var w = a != u && a != v ? a : b != u && b != v ? b : c;
                var r = positions[w] - positions[u];
                var angle = Fun.Atan2(r.Dot(ax2), r.Dot(ax1));
                var forward = tris[t * 3 + he % 3] == u; // this half-edge starts at u
                return (He: he, Angle: angle, Forward: forward);
            }).ToArray();
            Array.Sort(around, (x, y) => (x.Angle - y.Angle).Abs() < 1e-9
                ? x.Forward.CompareTo(y.Forward) : x.Angle.CompareTo(y.Angle));

            var paired = new bool[around.Length];
            var made = true;
            while (made)
            {
                made = false;
                for (var i = 0; i < around.Length; i++)
                {
                    if (paired[i]) continue;
                    var j = (i + 1) % around.Length;
                    if (paired[j] || i == j) continue;
                    if (!around[i].Forward && around[j].Forward)
                    {
                        twin[around[i].He] = around[j].He;
                        twin[around[j].He] = around[i].He;
                        paired[i] = paired[j] = true;
                        made = true;
                    }
                }
            }
        }

        #endregion

        #region component emit

        private static PolyMesh? BuildComponent(List<int> triVerts, V3d[] positions)
        {
            if (triVerts.Count < 3) return null;
            // local remap of the vertices this component uses
            var local = new Dictionary<int, int>();
            var localPos = new List<V3d>();
            int L(int g)
            {
                if (!local.TryGetValue(g, out var li)) { li = localPos.Count; local[g] = li; localPos.Add(positions[g]); }
                return li;
            }
            var localList = new List<int>(triVerts.Count);
            for (var i = 0; i < triVerts.Count; i++) localList.Add(L(triVerts[i]));

            // orient the component internally (the global pass cannot cross the
            // non-manifold seams that separated the components)
            OrientConsistently(localList);
            var via = localList.ToArray();

            // split pinch vertices: group each vertex's incident corners by
            // link-connectivity through paired half-edges (a single vertex
            // where two cones meet becomes two coincident vertices)
            via = SplitPinchVertices(via, localPos);

            // orient outward: flip the whole component if its signed volume is negative
            if (SignedVolume(via, localPos) < 0)
                for (var t = 0; t < via.Length; t += 3) (via[t + 1], via[t + 2]) = (via[t + 2], via[t + 1]);

            var fia = new int[via.Length / 3 + 1].SetByIndex(i => i * 3);
            var pm = new PolyMesh
            {
                PositionArray = localPos.ToArray(),
                FirstIndexArray = fia,
                VertexIndexArray = via,
            };
            // only return genuinely valid manifolds; drop anything we could not close cleanly
            return ManifoldChecks.FindManifoldViolation(fia, via, localPos.Count) == null ? pm : null;
        }

        /// <summary>
        /// Duplicates a vertex once per connected fan when its incident corners
        /// form more than one link-cycle (a pinch point). Corners are grouped
        /// by union-find across paired half-edges sharing the vertex.
        /// </summary>
        private static int[] SplitPinchVertices(int[] via, List<V3d> localPos)
        {
            var h = via.Length;
            var twin = PairHalfEdges(via.ToList(), localPos.ToArray());
            var group = new int[h].SetByIndex(i => i);
            int Find(int i) { while (group[i] != i) { group[i] = group[group[i]]; i = group[i]; } return i; }
            void Union(int i, int j) { var a = Find(i); var b = Find(j); if (a != b) group[a.Max(b)] = a.Min(b); }

            for (var he = 0; he < h; he++)
            {
                var tw = twin[he];
                if (tw < 0) continue;
                var t = he / 3; var k = he % 3;
                var tt = tw / 3; var kk = tw % 3;
                var u = via[t * 3 + k]; var vv = via[t * 3 + (k + 1) % 3];
                // paired half-edges share vertices u and v; unify their corners
                // (the corner at u on this face with the corner at u on the twin)
                UnifyCorner(via, t, u, tt, u, Union);
                UnifyCorner(via, t, vv, tt, vv, Union);
            }

            // new vertex per (original vertex, corner group)
            var newIndex = new Dictionary<(int, int), int>();
            var newPos = new List<V3d>();
            var outV = new int[h];
            for (var c = 0; c < h; c++)
            {
                var g = Find(c);
                var vtx = via[c];
                var keyVg = (vtx, g);
                if (!newIndex.TryGetValue(keyVg, out var ni))
                { ni = newPos.Count; newIndex[keyVg] = ni; newPos.Add(localPos[vtx]); }
                outV[c] = ni;
            }
            localPos.Clear();
            localPos.AddRange(newPos);
            return outV;
        }

        private static void UnifyCorner(int[] via, int t0, int v0, int t1, int v1, Action<int, int> union)
        {
            var c0 = via[t0 * 3] == v0 ? t0 * 3 : via[t0 * 3 + 1] == v0 ? t0 * 3 + 1 : t0 * 3 + 2;
            var c1 = via[t1 * 3] == v1 ? t1 * 3 : via[t1 * 3 + 1] == v1 ? t1 * 3 + 1 : t1 * 3 + 2;
            union(c0, c1);
        }

        private static double SignedVolume(int[] via, List<V3d> pos)
        {
            var c = V3d.Zero;
            foreach (var p in pos) c += p;
            c /= pos.Count.Max(1);
            var sum = 0.0;
            for (var t = 0; t < via.Length; t += 3)
            {
                var a = pos[via[t]] - c; var b = pos[via[t + 1]] - c; var d = pos[via[t + 2]] - c;
                sum += a.Dot(b.Cross(d));
            }
            return sum / 6.0;
        }

        #endregion

        private static long UKey(int a, int b) => a < b ? ((long)a << 32) | (uint)b : ((long)b << 32) | (uint)a;
        private static long DKey(int a, int b) => ((long)a << 32) | (uint)b;
    }
}
