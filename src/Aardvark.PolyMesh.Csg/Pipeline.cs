using System;
using System.Collections.Generic;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>Classification of a fragment against the other solid.</summary>
    internal enum FragLabel : byte
    {
        Outside,
        Inside,
        /// <summary>Coplanar with a facet of the other solid, normals aligned.</summary>
        OnSame,
        /// <summary>Coplanar with a facet of the other solid, normals opposed.</summary>
        OnOpposite,
    }

    /// <summary>One output triangle fragment: canonical vertex ids + the kernel triangle it stems from.</summary>
    internal readonly struct Fragment
    {
        public readonly int V0, V1, V2;
        public readonly int Parent;
        public Fragment(int v0, int v1, int v2, int parent) { V0 = v0; V1 = v1; V2 = v2; Parent = parent; }
    }

    /// <summary>
    /// The arrangement pipeline: weld → classify → broad phase → intersection
    /// segments → per-face subdivision → inside/outside labeling.
    /// See DESIGN.md for the rationale of each stage.
    /// </summary>
    internal sealed class Pipeline
    {
        private readonly Kernel m_kernel;
        private readonly Eps m_eps;

        /// <summary>kernel vertex id → canonical vertex id (weld representative)</summary>
        public int[] Canon = Array.Empty<int>();
        public readonly List<Fragment> Fragments = new();
        public readonly List<FragLabel> Labels = new();

        private readonly Dictionary<long, Sign3> m_signCache = new();
        private readonly Dictionary<(int, int, int), int> m_cutCache = new(); // (edgeMin, edgeMax, planeId) -> vid

        // spatial hash over all kernel vertices so coincident derived points
        // (triple-plane corners reached via different edge/plane cuts) weld to
        // one canonical vertex; the grid is a candidate filter only,
        // correctness comes from AreCoincident
        private readonly Dictionary<(long, long, long), List<int>> m_vertexGrid = new();
        private double m_gridH = 1.0;
        private readonly Dictionary<(int, int), HashSet<int>> m_edgePoints = new(); // canonical edge -> points on it
        private readonly Dictionary<int, HashSet<(int, int)>> m_faceConstraints = new(); // kernel tri -> segments
        private readonly HashSet<(int, int)>[] m_barriers = { new(), new() }; // per mesh: constraint sub-edges
        private readonly Dictionary<int, List<(int Partner, bool Same)>> m_coplanar = new(); // tri -> overlapping coplanar tris of the other mesh

        public Pipeline(Kernel kernel)
        {
            kernel.UpdateSceneScale();
            m_kernel = kernel;
            m_eps = kernel.Eps;
        }

        public void Run()
        {
            WeldVertices();
            var pairs = BroadPhase();
            foreach (var (ta, tb) in pairs) ProcessPair(ta, tb);
            Subdivide();
            Classify();
        }

        #region welding

        private void WeldVertices()
        {
            var n = m_kernel.Positions.Count;
            var parent = new int[n].SetByIndex(i => i);
            int Find(int i) { while (parent[i] != i) { parent[i] = parent[parent[i]]; i = parent[i]; } return i; }
            void Union(int i, int j)
            {
                var ri = Find(i); var rj = Find(j);
                if (ri == rj) return;
                if (ri < rj) parent[rj] = ri; else parent[ri] = rj;
            }

            var maxMag = 0.0;
            for (var i = 0; i < n; i++) maxMag = maxMag.Max(m_kernel.Positions[i].NormMax);
            var h = (2 * m_eps.Relative * maxMag).Max(1e-300);

            // hash grid is a candidate filter only: any coincident pair is within
            // one cell in every axis, correctness comes from AreCoincident
            var grid = new Dictionary<(long, long, long), List<int>>();
            for (var i = 0; i < n; i++)
            {
                var p = m_kernel.Positions[i];
                var cx = (long)Fun.Floor(p.X / h); var cy = (long)Fun.Floor(p.Y / h); var cz = (long)Fun.Floor(p.Z / h);
                for (var dx = -1L; dx <= 1; dx++)
                    for (var dy = -1L; dy <= 1; dy++)
                        for (var dz = -1L; dz <= 1; dz++)
                        {
                            if (!grid.TryGetValue((cx + dx, cy + dy, cz + dz), out var cell)) continue;
                            foreach (var j in cell)
                                if (m_eps.AreCoincident(p, m_kernel.Positions[j])) Union(i, j);
                        }
                grid.GetOrCreate((cx, cy, cz), _ => new List<int>()).Add(i);
            }

            Canon = new int[n].SetByIndex(i => Find(i));

            // reject same-mesh welds: features below tolerance in one input
            for (var i = 0; i < n; i++)
            {
                var r = Canon[i];
                if (r != i && m_kernel.VertexSourceMesh(r) == m_kernel.VertexSourceMesh(i))
                    throw new CsgInputException(
                        $"input mesh {(m_kernel.VertexSourceMesh(i) == 0 ? "A" : "B")} contains distinct vertices closer than tolerance " +
                        $"(vertices {i - m_kernel.VertexOffset[m_kernel.VertexSourceMesh(i)]} and {r - m_kernel.VertexOffset[m_kernel.VertexSourceMesh(r)]})");
            }

            for (var t = 0; t < m_kernel.TriangleCount; t++)
            {
                m_kernel.T0[t] = Canon[m_kernel.T0[t]];
                m_kernel.T1[t] = Canon[m_kernel.T1[t]];
                m_kernel.T2[t] = Canon[m_kernel.T2[t]];
            }

            // persistent vertex grid over canonical vertices; sized so that a
            // generation-1 coincidence tolerance still fits one neighbor cell
            m_gridH = (16 * m_eps.Relative * maxMag).Max(1e-300);
            for (var i = 0; i < n; i++)
                if (Canon[i] == i) GridAdd(i);
        }

        private (long, long, long) GridCell(in V3d p)
            => ((long)Fun.Floor(p.X / m_gridH), (long)Fun.Floor(p.Y / m_gridH), (long)Fun.Floor(p.Z / m_gridH));

        private void GridAdd(int vid)
            => m_vertexGrid.GetOrCreate(GridCell(m_kernel.Positions[vid]), _ => new List<int>()).Add(vid);

        /// <summary>Existing kernel vertex coincident with p (generation-1 tolerance), or -1.</summary>
        private int GridFindCoincident(in V3d p)
        {
            var (cx, cy, cz) = GridCell(p);
            for (var dx = -1L; dx <= 1; dx++)
                for (var dy = -1L; dy <= 1; dy++)
                    for (var dz = -1L; dz <= 1; dz++)
                    {
                        if (!m_vertexGrid.TryGetValue((cx + dx, cy + dy, cz + dz), out var cell)) continue;
                        foreach (var j in cell)
                            if (m_eps.AreCoincident(p, m_kernel.Positions[j], 1)) return j;
                    }
            return -1;
        }

        #endregion

        #region classification cache

        private Sign3 Sign(int planeId, int vid)
        {
            var key = ((long)planeId << 32) | (uint)vid;
            if (m_signCache.TryGetValue(key, out var s)) return s;
            s = m_eps.HeightSign(m_kernel.Planes[planeId], m_kernel.Positions[vid], m_kernel.Generation[vid]);
            m_signCache[key] = s;
            return s;
        }

        #endregion

        #region broad phase

        private List<(int, int)> BroadPhase()
        {
            var boxes = new Box3d[2][];
            var triOf = new int[2][];
            for (var m = 0; m < 2; m++)
            {
                var list = new List<int>();
                for (var t = 0; t < m_kernel.TriangleCount; t++)
                    if (m_kernel.TriMesh[t] == m) list.Add(t);
                triOf[m] = list.ToArray();
                boxes[m] = new Box3d[list.Count];
                for (var i = 0; i < list.Count; i++)
                {
                    var t = list[i];
                    var p0 = m_kernel.Positions[m_kernel.T0[t]];
                    var p1 = m_kernel.Positions[m_kernel.T1[t]];
                    var p2 = m_kernel.Positions[m_kernel.T2[t]];
                    var box = new Box3d(p0, p1, p2);
                    var slack = 8 * m_eps.Relative * box.Min.NormMax.Max(box.Max.NormMax);
                    boxes[m][i] = box.EnlargedBy(slack);
                }
            }
            var pairs = new List<(int, int)>();
            new CsgBvh(boxes[0]).ForEachIntersectingPair(new CsgBvh(boxes[1]),
                (i, j) => pairs.Add((triOf[0][i], triOf[1][j])));
            return pairs;
        }

        #endregion

        #region narrow phase: intersection segments

        private readonly struct CrossPt
        {
            public readonly int Vid;            // existing vertex, or -1
            public readonly int EdgeA, EdgeB;   // edge to cut (canonical ids) when Vid < 0
            public readonly V3d Pos;
            public CrossPt(int vid, V3d pos) { Vid = vid; EdgeA = EdgeB = -1; Pos = pos; }
            public CrossPt(int ea, int eb, V3d pos) { Vid = -1; EdgeA = ea; EdgeB = eb; Pos = pos; }
        }

        private void ProcessPair(int ta, int tb)
        {
            var pa = m_kernel.TriPlane[ta];
            var pb = m_kernel.TriPlane[tb];

            Span<int> va = stackalloc int[] { m_kernel.T0[ta], m_kernel.T1[ta], m_kernel.T2[ta] };
            Span<int> vb = stackalloc int[] { m_kernel.T0[tb], m_kernel.T1[tb], m_kernel.T2[tb] };
            Span<Sign3> sb = stackalloc Sign3[3];
            Span<Sign3> sa = stackalloc Sign3[3];
            for (var i = 0; i < 3; i++) sb[i] = Sign(pa, vb[i]);
            if (AllStrict(sb, Sign3.Above) || AllStrict(sb, Sign3.Below)) return;
            for (var i = 0; i < 3; i++) sa[i] = Sign(pb, va[i]);
            if (AllStrict(sa, Sign3.Above) || AllStrict(sa, Sign3.Below)) return;

            if (AllOn(sa) && AllOn(sb))
            {
                // tangent contact without 2D interior overlap (solids sharing a
                // plane strip, an edge, a corner) needs no arrangement at all
                if (!CoplanarInteriorsOverlap(va, vb, pa)) return;
                // overlapping coplanar facets: subdivision happens via the
                // side-face pairs (every facet boundary edge also belongs to a
                // non-coplanar face); here we only record coverage for labeling
                var same = m_kernel.Planes[pa].Normal.Dot(m_kernel.Planes[pb].Normal) > 0;
                m_coplanar.GetOrCreate(ta, _ => new List<(int, bool)>()).Add((tb, same));
                m_coplanar.GetOrCreate(tb, _ => new List<(int, bool)>()).Add((ta, same));
                return;
            }

            var crossA = CrossingPoints(va, sa, pb);
            var crossB = CrossingPoints(vb, sb, pa);
            if (crossA.Count < 2 || crossB.Count < 2) return; // point touch or grazing

            // both segments lie on the intersection line of the two planes;
            // the actual intersection is the overlap of the two intervals, and
            // its endpoints are always existing crossing points of one triangle
            var dir = m_kernel.Planes[pa].Normal.Cross(m_kernel.Planes[pb].Normal);
            var (loA, hiA) = Interval(crossA, dir);
            var (loB, hiB) = Interval(crossB, dir);
            var (lo, loCutPlane) = loA.T > loB.T ? (loA, pb) : (loB, pa);
            var (hi, hiCutPlane) = hiA.T < hiB.T ? (hiA, pb) : (hiB, pa);
            if (lo.T >= hi.T) return;
            if (m_eps.AreCoincident(lo.P.Pos, hi.P.Pos, 1)) return; // point touch

            var v0 = Materialize(lo.P, loCutPlane);
            var v1 = Materialize(hi.P, hiCutPlane);
            if (v0 == v1) return;

            AddConstraint(ta, v0, v1);
            AddConstraint(tb, v0, v1);
        }

        private static bool AllStrict(Span<Sign3> s, Sign3 v) => s[0] == v && s[1] == v && s[2] == v;
        private static bool AllOn(Span<Sign3> s) => s[0] == Sign3.On && s[1] == Sign3.On && s[2] == Sign3.On;

        /// <summary>2D separating-edge test for two coplanar triangles; touching along boundary counts as not overlapping.</summary>
        private bool CoplanarInteriorsOverlap(Span<int> va, Span<int> vb, int plane)
        {
            var n = m_kernel.Planes[plane].Normal;
            Span<V2d> a = stackalloc V2d[3];
            Span<V2d> b = stackalloc V2d[3];
            for (var i = 0; i < 3; i++)
            {
                a[i] = Triangulator.ProjectDominant(n, m_kernel.Positions[va[i]]);
                b[i] = Triangulator.ProjectDominant(n, m_kernel.Positions[vb[i]]);
            }
            if (!MakeCcw(a) || !MakeCcw(b)) return false; // degenerate projection
            return !HasSeparatingEdge(a, b) && !HasSeparatingEdge(b, a);
        }

        private bool MakeCcw(Span<V2d> t)
        {
            switch (m_eps.AreaSign(t[0], t[1], t[2]))
            {
                case Sign3.Above: return true;
                case Sign3.Below: (t[1], t[2]) = (t[2], t[1]); return true;
                case Sign3.On: return false;
                default: throw new InvalidOperationException();
            }
        }

        private bool HasSeparatingEdge(Span<V2d> p, Span<V2d> q)
        {
            for (var i = 0; i < 3; i++)
            {
                var u = p[i]; var v = p[(i + 1) % 3];
                var separated = true;
                for (var j = 0; j < 3 && separated; j++)
                    if (m_eps.AreaSign(u, v, q[j]) == Sign3.Above) separated = false;
                if (separated) return true;
            }
            return false;
        }

        /// <summary>Crossing points of a triangle with the other triangle's plane: On-vertices and strict sign-change edge cuts.</summary>
        private List<CrossPt> CrossingPoints(Span<int> v, Span<Sign3> s, int plane)
        {
            var result = new List<CrossPt>(2);
            for (var i = 0; i < 3; i++)
            {
                if (s[i] == Sign3.On)
                {
                    var vi = v[i];
                    if (!result.Exists(c => c.Vid == vi))
                        result.Add(new CrossPt(vi, m_kernel.Positions[vi]));
                }
                var j = (i + 1) % 3;
                if ((s[i] == Sign3.Above && s[j] == Sign3.Below) || (s[i] == Sign3.Below && s[j] == Sign3.Above))
                {
                    var p = m_kernel.Planes[plane];
                    var pi = m_kernel.Positions[v[i]];
                    var pj = m_kernel.Positions[v[j]];
                    var hi = p.Normal.Dot(pi) - p.Distance;
                    var hj = p.Normal.Dot(pj) - p.Distance;
                    var t = hi / (hi - hj);
                    result.Add(new CrossPt(v[i], v[j], pi + t * (pj - pi)));
                }
            }
            return result;
        }

        private static ((double T, CrossPt P) Lo, (double T, CrossPt P) Hi) Interval(List<CrossPt> cross, V3d dir)
        {
            var lo = (T: double.MaxValue, P: default(CrossPt));
            var hi = (T: double.MinValue, P: default(CrossPt));
            foreach (var c in cross)
            {
                var t = dir.Dot(c.Pos);
                if (t < lo.T) lo = (t, c);
                if (t > hi.T) hi = (t, c);
            }
            return (lo, hi);
        }

        /// <summary>Turns a crossing point into a kernel vertex: existing vertex as-is, edge cuts memoized per (edge, plane) with commit-to-endpoint.</summary>
        private int Materialize(CrossPt c, int cutPlane)
        {
            if (c.Vid >= 0) return c.Vid;
            var key = c.EdgeA < c.EdgeB ? (c.EdgeA, c.EdgeB, cutPlane) : (c.EdgeB, c.EdgeA, cutPlane);
            if (m_cutCache.TryGetValue(key, out var vid)) return vid;

            if (m_eps.AreCoincident(c.Pos, m_kernel.Positions[c.EdgeA], 1)) vid = c.EdgeA;
            else if (m_eps.AreCoincident(c.Pos, m_kernel.Positions[c.EdgeB], 1)) vid = c.EdgeB;
            else
            {
                // triple-plane corners are reached via several distinct
                // (edge, plane) cuts: weld onto a coincident existing vertex
                vid = GridFindCoincident(c.Pos);
                if (vid < 0)
                {
                    vid = m_kernel.Positions.Count;
                    m_kernel.Positions.Add(c.Pos);
                    m_kernel.Generation.Add(1);
                    GridAdd(vid);
                }
                RegisterEdgePoint(c.EdgeA, c.EdgeB, vid);
            }
            m_cutCache[key] = vid;
            return vid;
        }

        private void RegisterEdgePoint(int a, int b, int vid)
            => m_edgePoints.GetOrCreate(SortedEdge(a, b), _ => new HashSet<int>()).Add(vid);

        private static (int, int) SortedEdge(int a, int b) => a < b ? (a, b) : (b, a);

        private void AddConstraint(int tri, int v0, int v1)
            => m_faceConstraints.GetOrCreate(tri, _ => new HashSet<(int, int)>()).Add(SortedEdge(v0, v1));

        #endregion

        #region subdivision

        private void Subdivide()
        {
            // T-junction pass: constraint endpoints that lie on a face's
            // boundary edge are registered on that (shared) edge so the
            // neighbor subdivides identically
            foreach (var (tri, constraints) in m_faceConstraints)
            {
                var plane = m_kernel.Planes[m_kernel.TriPlane[tri]];
                Span<int> v = stackalloc int[] { m_kernel.T0[tri], m_kernel.T1[tri], m_kernel.T2[tri] };
                Span<V2d> p = stackalloc V2d[3];
                for (var i = 0; i < 3; i++) p[i] = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[v[i]]);

                foreach (var (ca, cb) in constraints)
                {
                    foreach (var e in new[] { ca, cb })
                    {
                        var q = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[e]);
                        for (var i = 0; i < 3; i++)
                        {
                            var j = (i + 1) % 3;
                            if (e == v[i] || e == v[j]) continue;
                            if (m_eps.AreaSign(p[i], p[j], q, 1) != Sign3.On) continue;
                            var d = p[j] - p[i]; var w = q - p[i];
                            var dot = d.Dot(w);
                            if (dot <= 0 || dot >= d.LengthSquared) continue;
                            RegisterEdgePoint(v[i], v[j], e);
                        }
                    }
                }
            }

            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
            {
                var constraints = m_faceConstraints.GetOrDefault(tri);
                var boundary = BoundaryPoints(tri);
                if (constraints == null && boundary == null)
                {
                    Fragments.Add(new Fragment(m_kernel.T0[tri], m_kernel.T1[tri], m_kernel.T2[tri], tri));
                    continue;
                }

                m_diagTri = tri;
                var plane = m_kernel.Planes[m_kernel.TriPlane[tri]];
                V2d Proj(int vid) => Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[vid]);

                var cdt = new FaceCdt(m_eps,
                    m_kernel.T0[tri], Proj(m_kernel.T0[tri]),
                    m_kernel.T1[tri], Proj(m_kernel.T1[tri]),
                    m_kernel.T2[tri], Proj(m_kernel.T2[tri]));

                if (boundary != null)
                    foreach (var vid in boundary)
                    {
                        try { cdt.InsertPoint(vid, Proj(vid)); }
                        catch (CsgVerificationException e) { throw new CsgVerificationException(Diag(e, vid)); }
                    }
                if (constraints != null)
                {
                    foreach (var (ca, cb) in constraints)
                    {
                        try { cdt.InsertPoint(ca, Proj(ca)); cdt.InsertPoint(cb, Proj(cb)); }
                        catch (CsgVerificationException e) { throw new CsgVerificationException(Diag(e, ca, cb)); }
                    }
                    foreach (var (ca, cb) in constraints) cdt.AddConstraint(ca, cb);
                }

                List<(int, int, int)> tris;
                List<(int, int)> constraintEdges;
                try
                {
                    (tris, constraintEdges) = cdt.Triangulate();
                }
                catch (CsgVerificationException e)
                {
                    throw new CsgVerificationException(Diag(e));
                }
                foreach (var (a, b, c) in tris) Fragments.Add(new Fragment(a, b, c, tri));
                var barrier = m_barriers[m_kernel.TriMesh[tri]];
                foreach (var (a, b) in constraintEdges) barrier.Add(SortedEdge(a, b));
            }
        }

        private string Diag(Exception e, params int[] vids)
        {
            var tri = m_diagTri;
            var s = $"{e.Message} [tri {tri} mesh {m_kernel.TriMesh[tri]} face {m_kernel.TriFace[tri]} " +
                $"corners ({m_kernel.T0[tri]}:{m_kernel.Positions[m_kernel.T0[tri]]}, {m_kernel.T1[tri]}:{m_kernel.Positions[m_kernel.T1[tri]]}, {m_kernel.T2[tri]}:{m_kernel.Positions[m_kernel.T2[tri]]})";
            foreach (var v in vids) s += $" point {v}:{m_kernel.Positions[v]} gen {m_kernel.Generation[v]}";
            return s + "]";
        }

        private int m_diagTri;

        private List<int>? BoundaryPoints(int tri)
        {
            List<int>? result = null;
            Span<int> v = stackalloc int[] { m_kernel.T0[tri], m_kernel.T1[tri], m_kernel.T2[tri] };
            for (var i = 0; i < 3; i++)
            {
                var pts = m_edgePoints.GetOrDefault(SortedEdge(v[i], v[(i + 1) % 3]));
                if (pts == null) continue;
                result ??= new List<int>();
                foreach (var p in pts) if (!result.Contains(p)) result.Add(p);
            }
            return result;
        }

        #endregion

        #region inside/outside classification

        private static readonly V3d[] s_rayDirs = new[]
        {
            new V3d(0.2971, 0.5843, 0.7552), new V3d(-0.6312, 0.4489, 0.6324),
            new V3d(0.8412, -0.3811, 0.3832), new V3d(0.1213, 0.9313, -0.3434),
            new V3d(-0.4141, -0.5555, 0.7212), new V3d(0.7717, 0.2323, -0.5919),
            new V3d(-0.2626, 0.7878, 0.5566), new V3d(0.5151, -0.6464, -0.5633),
            new V3d(0.9191, 0.1919, 0.3468), new V3d(-0.7373, -0.1717, 0.6534),
            new V3d(0.3737, 0.6161, -0.6935), new V3d(-0.1818, -0.8888, 0.4207),
        }.Map(v => v.Normalized);

        private void Classify()
        {
            // fragment adjacency per mesh across shared (canonical) sub-edges,
            // with intersection-curve edges acting as barriers
            var edgeToFragments = new Dictionary<(int, int), List<int>>[] { new(), new() };
            for (var f = 0; f < Fragments.Count; f++)
            {
                var frag = Fragments[f];
                var mesh = m_kernel.TriMesh[frag.Parent];
                foreach (var e in FragmentEdges(frag))
                    edgeToFragments[mesh].GetOrCreate(e, _ => new List<int>()).Add(f);
            }

            Labels.Clear();
            for (var f = 0; f < Fragments.Count; f++) Labels.Add(FragLabel.Outside);
            var labeled = new bool[Fragments.Count];

            // coplanar-covered fragments are labeled directly (their coverage
            // boundary is made of constraint edges, so they are flood-isolated)
            for (var f = 0; f < Fragments.Count; f++)
            {
                var frag = Fragments[f];
                var partners = m_coplanar.GetOrDefault(frag.Parent);
                if (partners == null) continue;
                var centroid = (m_kernel.Positions[frag.V0] + m_kernel.Positions[frag.V1] + m_kernel.Positions[frag.V2]) / 3.0;
                foreach (var (partner, same) in partners)
                {
                    if (!CoplanarCovers(partner, centroid)) continue;
                    Labels[f] = same ? FragLabel.OnSame : FragLabel.OnOpposite;
                    labeled[f] = true;
                    break;
                }
            }

            for (var seedFrag = 0; seedFrag < Fragments.Count; seedFrag++)
            {
                if (labeled[seedFrag]) continue;
                var mesh = m_kernel.TriMesh[Fragments[seedFrag].Parent];
                var region = new List<int>();
                var stack = new Stack<int>();
                stack.Push(seedFrag);
                labeled[seedFrag] = true;
                while (stack.Count > 0)
                {
                    var f = stack.Pop();
                    region.Add(f);
                    foreach (var e in FragmentEdges(Fragments[f]))
                    {
                        if (m_barriers[mesh].Contains(e)) continue;
                        foreach (var g in edgeToFragments[mesh][e])
                        {
                            if (labeled[g]) continue;
                            labeled[g] = true;
                            stack.Push(g);
                        }
                    }
                }

                var inside = RegionIsInsideOther(region, 1 - mesh);
                foreach (var f in region) Labels[f] = inside ? FragLabel.Inside : FragLabel.Outside;
            }
        }

        /// <summary>
        /// True if p (a point in the shared plane) lies inside or on the
        /// partner triangle. On counts as inside: a fragment centroid can sit
        /// exactly on a partner's internal diagonal (matching triangulations of
        /// coincident quads), while the outer boundary of a covered region is
        /// always made of constraint edges, which fragments never straddle —
        /// so an On answer here can only mean "on an interior edge of the
        /// covered region".
        /// </summary>
        private bool CoplanarCovers(int partner, in V3d p)
        {
            var n = m_kernel.Planes[m_kernel.TriPlane[partner]].Normal;
            Span<V2d> t = stackalloc V2d[3];
            t[0] = Triangulator.ProjectDominant(n, m_kernel.Positions[m_kernel.T0[partner]]);
            t[1] = Triangulator.ProjectDominant(n, m_kernel.Positions[m_kernel.T1[partner]]);
            t[2] = Triangulator.ProjectDominant(n, m_kernel.Positions[m_kernel.T2[partner]]);
            if (!MakeCcw(t)) return false;
            var q = Triangulator.ProjectDominant(n, p);
            return m_eps.AreaSign(t[0], t[1], q, 1) != Sign3.Below
                && m_eps.AreaSign(t[1], t[2], q, 1) != Sign3.Below
                && m_eps.AreaSign(t[2], t[0], q, 1) != Sign3.Below;
        }

        private IEnumerable<(int, int)> FragmentEdges(Fragment f)
        {
            yield return SortedEdge(f.V0, f.V1);
            yield return SortedEdge(f.V1, f.V2);
            yield return SortedEdge(f.V2, f.V0);
        }

        private bool RegionIsInsideOther(List<int> region, int otherMesh)
        {
            // try region fragments in order; per fragment try the ray directions:
            // any unambiguous parity decides
            foreach (var f in region)
            {
                var frag = Fragments[f];
                var o = (m_kernel.Positions[frag.V0] + m_kernel.Positions[frag.V1] + m_kernel.Positions[frag.V2]) / 3.0;
                foreach (var dir in s_rayDirs)
                {
                    var parity = RayParity(o, dir, otherMesh);
                    if (parity.HasValue) return parity.Value;
                }
            }
            throw new CsgVerificationException("could not classify a surface region (all ray casts ambiguous)");
        }

        /// <summary>Parity of ray/other-mesh crossings; null when any hit is eps-ambiguous.</summary>
        private bool? RayParity(V3d o, V3d dir, int otherMesh)
        {
            var count = 0;
            for (var t = 0; t < m_kernel.TriangleCount; t++)
            {
                if (m_kernel.TriMesh[t] != otherMesh) continue;
                var plane = m_kernel.Planes[m_kernel.TriPlane[t]];
                var denom = plane.Normal.Dot(dir);
                var h = plane.Normal.Dot(o) - plane.Distance;
                if (denom.Abs() < 1e-9)
                {
                    if (m_eps.HeightSign(plane, o, 1) == Sign3.On) return null; // ray (nearly) in plane near origin
                    continue;
                }
                var s = -h / denom;
                if (s <= 0) continue;
                var hit = o + s * dir;
                if (m_eps.AreCoincident(hit, o, 1)) return null; // origin on the other surface

                var p0 = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[m_kernel.T0[t]]);
                var p1 = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[m_kernel.T1[t]]);
                var p2 = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[m_kernel.T2[t]]);
                var q = Triangulator.ProjectDominant(plane.Normal, hit);
                var s0 = m_eps.AreaSign(p0, p1, q, 1);
                var s1 = m_eps.AreaSign(p1, p2, q, 1);
                var s2 = m_eps.AreaSign(p2, p0, q, 1);
                if (s0 == Sign3.Below || s1 == Sign3.Below || s2 == Sign3.Below) continue; // outside triangle
                if (s0 == Sign3.On || s1 == Sign3.On || s2 == Sign3.On) return null;       // grazing edge/vertex
                count++;
            }
            return (count & 1) == 1;
        }

        #endregion
    }
}
