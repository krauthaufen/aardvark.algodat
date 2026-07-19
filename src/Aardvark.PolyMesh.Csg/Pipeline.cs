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
        /// <summary>per (fragment, other mesh) classification, [frag * MeshCount + mesh]; self slot unused</summary>
        private byte[] m_rel = Array.Empty<byte>();

        public FragLabel Label(int frag, int otherMesh)
            => (FragLabel)m_rel[frag * m_kernel.MeshCount + otherMesh];

        public bool HasCoincidentContact => m_coplanar.Count > 0;

        private readonly Dictionary<(int, int, int), int> m_cutCache = new(); // (edgeMin, edgeMax, planeId) -> vid

        // spatial hash over all kernel vertices so coincident derived points
        // (triple-plane corners reached via different edge/plane cuts) weld to
        // one canonical vertex; the grid is a candidate filter only,
        // correctness comes from AreCoincident
        private readonly Dictionary<long, int> m_vertexGridHeads = new();
        private readonly List<int> m_vertexGridNext = new();
        private double m_gridH = 1.0;
        // coarse grid for wide (grazing-conditioned) coincidence radii
        private readonly Dictionary<long, int> m_coarseHeads = new();
        private readonly List<int> m_coarseNext = new();
        private double m_coarseH = 1.0;

        private static long CellKey(long x, long y, long z)
        {
            unchecked
            {
                var h = (ulong)x * 0x9E3779B97F4A7C15UL;
                h ^= (ulong)y * 0xC2B2AE3D27D4EB4FUL;
                h ^= (ulong)z * 0x165667B19E3779F9UL;
                h ^= h >> 29;
                return (long)h;
            }
        }
        private readonly Dictionary<(int, int), HashSet<int>> m_edgePoints = new(); // canonical edge -> points on it
        private readonly Dictionary<int, HashSet<(int, int)>> m_faceConstraints = new(); // kernel tri -> segments
        private HashSet<long>[] m_barriers = Array.Empty<HashSet<long>>(); // per mesh: constraint sub-edges (packed keys)
        private readonly Dictionary<int, List<(int Partner, bool Same)>> m_coplanar = new(); // tri -> overlapping coplanar tris of the other mesh
        private CsgBvh[] m_bvh = Array.Empty<CsgBvh>();
        private int[][] m_triOf = Array.Empty<int[]>();
        private int[] m_adjNbr = Array.Empty<int>();
        private int[] m_adjOffsets = Array.Empty<int>();

        /// <summary>optional pre-built solids providing cached BVHs (index-aligned with meshes; entries may be null)</summary>
        private readonly CsgMesh?[] m_prepared;

        private readonly int m_maxThreads;

        public Pipeline(Kernel kernel, CsgMesh?[]? prepared = null, int maxThreads = 1)
        {
            m_maxThreads = maxThreads.Max(1);
            kernel.UpdateSceneScale();
            m_kernel = kernel;
            m_eps = kernel.Eps;
            m_prepared = prepared ?? new CsgMesh?[kernel.MeshCount];
            m_barriers = new HashSet<long>[kernel.MeshCount]
                .SetByIndex(_ => new HashSet<long>(MixedLongComparer.Instance));
        }

        private static readonly string? s_debugFace = Environment.GetEnvironmentVariable("CSG_DEBUG_FACE");
        private System.Diagnostics.Stopwatch? m_perfSw;
        internal void Lap2(string stage)
        {
            if (m_perfSw == null) return;
            Console.WriteLine($"PERF     {stage}: {m_perfSw.Elapsed.TotalMilliseconds:0.0} ms");
            m_perfSw.Restart();
        }

        public void Run()
        {
            var perf = Environment.GetEnvironmentVariable("CSG_PERF") != null;
            var sw = perf ? System.Diagnostics.Stopwatch.StartNew() : null;
            m_perfSw = perf ? System.Diagnostics.Stopwatch.StartNew() : null;
            void Lap(string stage)
            {
                if (sw == null) return;
                Console.WriteLine($"PERF {stage}: {sw.Elapsed.TotalMilliseconds:0.0} ms");
                sw.Restart();
                m_perfSw!.Restart();
            }
            WeldVertices();
            Lap("weld");
            var pairs = BroadPhase();
            Lap($"broadphase ({pairs.Count} pairs)");
            // symbolic pair results in parallel (pure sign/interval math),
            // materialization (ids, welding, constraint registration) in
            // deterministic pair order
            var pairResults = new PairResult[pairs.Count];
            CsgParallel.For(0, pairs.Count, m_maxThreads, i =>
                pairResults[i] = ComputePair(pairs[i].Item1, pairs[i].Item2));
            for (var i = 0; i < pairs.Count; i++)
                MaterializePair(pairs[i].Item1, pairs[i].Item2, pairResults[i]);
            Lap("narrowphase");
            Subdivide();
            Lap($"subdivide ({Fragments.Count} fragments)");
            Classify();
            Lap("classify");
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
            var h = (3 * m_eps.Relative * maxMag).Max(1e-300);

            // prescreen: a vertex can only weld with one within tolerance of a
            // FOREIGN mesh's bounds; vertices far from every other mesh skip
            // the grid entirely (distant same-mesh coincidences cannot affect
            // the boolean — they are re-emitted exactly as they came in)
            var meshCount = m_kernel.MeshCount;
            var foreign = new Box3d[meshCount];
            var inflate = 4 * m_eps.Relative * maxMag;
            for (var m = 0; m < meshCount; m++)
            {
                var box = Box3d.Invalid;
                for (var o = 0; o < meshCount; o++)
                    if (o != m) box.ExtendBy(m_kernel.Bounds[o]);
                foreign[m] = box.EnlargedBy(inflate);
            }

            // hash grid is a candidate filter only: any coincident pair lies
            // within the tolerance box around p (usually a single cell),
            // correctness comes from AreCoincident; hashed cell keys may alias,
            // which only adds candidates
            // phase 1 (sequential): candidates near foreign meshes into the grid
            var heads = new Dictionary<long, int>(1024, MixedLongComparer.Instance);
            var next = new int[n];
            var candidates = new List<int>();
            for (var i = 0; i < n; i++)
            {
                var p = m_kernel.Positions[i];
                if (!foreign[m_kernel.VertexMesh[i]].Contains(p)) continue;
                candidates.Add(i);
                var key = CellKey((long)Fun.Floor(p.X / h), (long)Fun.Floor(p.Y / h), (long)Fun.Floor(p.Z / h));
                next[i] = heads.TryGetValue(key, out var head) ? head : -1;
                heads[key] = i;
            }

            // phase 2 (parallel): probe the complete grid, collect coincident
            // pairs; phase 3 (sequential): union them — components identical
            // to incremental insertion because every coincident pair is found
            var pairBags = new List<(int, int)>[Math.Max(1, m_maxThreads)];
            var chunk = (candidates.Count + pairBags.Length - 1) / Math.Max(1, pairBags.Length);
            CsgParallel.For(0, pairBags.Length, m_maxThreads, blk =>
            {
                var bag = pairBags[blk] = new List<(int, int)>();
                var lo = blk * chunk;
                var hi = Math.Min(lo + chunk, candidates.Count);
                for (var ci = lo; ci < hi; ci++)
                {
                    var i = candidates[ci];
                    var p = m_kernel.Positions[i];
                    var tol = m_eps.Relative * (p.NormMax + 2 * m_eps.Scene);
                    var cx0 = (long)Fun.Floor((p.X - tol) / h); var cx1 = (long)Fun.Floor((p.X + tol) / h);
                    var cy0 = (long)Fun.Floor((p.Y - tol) / h); var cy1 = (long)Fun.Floor((p.Y + tol) / h);
                    var cz0 = (long)Fun.Floor((p.Z - tol) / h); var cz1 = (long)Fun.Floor((p.Z + tol) / h);
                    for (var dx = cx0; dx <= cx1; dx++)
                        for (var dy = cy0; dy <= cy1; dy++)
                            for (var dz = cz0; dz <= cz1; dz++)
                            {
                                if (!heads.TryGetValue(CellKey(dx, dy, dz), out var j)) continue;
                                for (; j >= 0; j = next[j])
                                    if (j < i && m_eps.AreCoincident(p, m_kernel.Positions[j]))
                                        bag.Add((i, j));
                            }
                }
            });
            foreach (var bag in pairBags)
                if (bag != null)
                    foreach (var (i, j) in bag) Union(i, j);

            Lap2("weld-probe");
            Canon = new int[n].SetByIndex(i => Find(i));

            // reject same-mesh welds (features below tolerance) for user
            // input; trusted prepared solids may legitimately contain
            // geometrically coincident vertices (pinched self-touching results
            // of earlier operations) — those weld back together
            for (var i = 0; i < n; i++)
            {
                var r = Canon[i];
                if (r == i) continue;
                var mesh = m_kernel.VertexSourceMesh(i);
                if (mesh != m_kernel.VertexSourceMesh(r)) continue;
                if (mesh >= 0 && m_prepared[mesh] != null) continue;
                throw new CsgInputException(
                    $"input mesh {mesh} contains distinct vertices closer than tolerance " +
                    $"(vertices {i - m_kernel.VertexOffset[mesh]} and {r - m_kernel.VertexOffset[mesh]})");
            }

            for (var t = 0; t < m_kernel.TriangleCount; t++)
            {
                m_kernel.T0[t] = Canon[m_kernel.T0[t]];
                m_kernel.T1[t] = Canon[m_kernel.T1[t]];
                m_kernel.T2[t] = Canon[m_kernel.T2[t]];
            }

            // persistent vertex grid over canonical vertices; sized so that a
            // generation-1 coincidence tolerance still fits one neighbor cell
            m_gridH = (24 * m_eps.Relative * maxMag).Max(1e-300);
            m_coarseH = (4 * Eps.MaxFactor * m_eps.Relative * maxMag).Max(1e-300);
            for (var i = 0; i < n; i++)
                if (Canon[i] == i && foreign[m_kernel.VertexMesh[i]].Contains(m_kernel.Positions[i]))
                    GridAdd(i);
            Lap2("weld-canon+grid");
        }

        /// <summary>Block boundaries into a sorted key array, aligned so no run crosses a block.</summary>
        internal static int[] RunAlignedBlocks(long[] keys, int maxThreads)
        {
            var blocks = Math.Max(1, Math.Min(maxThreads, keys.Length / (1 << 14)));
            var starts = new List<int>(blocks + 1) { 0 };
            for (var b = 1; b < blocks; b++)
            {
                var at = (int)((long)keys.Length * b / blocks);
                while (at < keys.Length && at > starts[^1] && keys[at] == keys[at - 1]) at++;
                if (at > starts[^1] && at < keys.Length) starts.Add(at);
            }
            starts.Add(keys.Length);
            return starts.ToArray();
        }

        private void GridAdd(int vid)
        {
            var p = m_kernel.Positions[vid];
            var key = CellKey((long)Fun.Floor(p.X / m_gridH), (long)Fun.Floor(p.Y / m_gridH), (long)Fun.Floor(p.Z / m_gridH));
            while (m_vertexGridNext.Count <= vid) m_vertexGridNext.Add(-1);
            m_vertexGridNext[vid] = m_vertexGridHeads.TryGetValue(key, out var head) ? head : -1;
            m_vertexGridHeads[key] = vid;
            var ck = CellKey((long)Fun.Floor(p.X / m_coarseH), (long)Fun.Floor(p.Y / m_coarseH), (long)Fun.Floor(p.Z / m_coarseH));
            while (m_coarseNext.Count <= vid) m_coarseNext.Add(-1);
            m_coarseNext[vid] = m_coarseHeads.TryGetValue(ck, out var chead) ? chead : -1;
            m_coarseHeads[ck] = vid;
        }

        /// <summary>Existing kernel vertex coincident with p (given tolerance factor), or -1.</summary>
        private int GridFindCoincident(in V3d p, double factor = Eps.GenerationFactor)
        {
            var tol = m_eps.Relative * (p.NormMax + 2 * m_eps.Scene) * factor;
            var fine = factor <= 3 * Eps.GenerationFactor;
            var h = fine ? m_gridH : m_coarseH;
            var heads = fine ? m_vertexGridHeads : m_coarseHeads;
            var next = fine ? m_vertexGridNext : m_coarseNext;
            var cx0 = (long)Fun.Floor((p.X - tol) / h); var cx1 = (long)Fun.Floor((p.X + tol) / h);
            var cy0 = (long)Fun.Floor((p.Y - tol) / h); var cy1 = (long)Fun.Floor((p.Y + tol) / h);
            var cz0 = (long)Fun.Floor((p.Z - tol) / h); var cz1 = (long)Fun.Floor((p.Z + tol) / h);
            for (var dx = cx0; dx <= cx1; dx++)
                for (var dy = cy0; dy <= cy1; dy++)
                    for (var dz = cz0; dz <= cz1; dz++)
                    {
                        if (!heads.TryGetValue(CellKey(dx, dy, dz), out var j)) continue;
                        for (; j >= 0; j = next[j])
                            if (m_eps.AreCoincident(p, m_kernel.Positions[j], factor)) return j;
                    }
            return -1;
        }

        #endregion

        #region classification cache

        // NOTE: no cache — HeightSign is pure and deterministic, so
        // recomputation is exactly as consistent as memoization, and it makes
        // the parallel narrow phase read-only
        private Sign3 Sign(int planeId, int vid)
            => m_eps.HeightSign(m_kernel.Planes[planeId], m_kernel.Positions[vid], m_kernel.TolFactor[vid]);

        #endregion

        #region broad phase

        private List<(int, int)> BroadPhase()
        {
            var n = m_kernel.MeshCount;
            m_bvh = new CsgBvh[n];
            m_triOf = new int[n][];
            var slackScene = 8 * m_eps.Relative * (m_eps.Scene + 1e-300);
            for (var m = 0; m < n; m++)
            {
                var list = new List<int>();
                for (var t = 0; t < m_kernel.TriangleCount; t++)
                    if (m_kernel.TriMesh[t] == m) list.Add(t);
                m_triOf[m] = list.ToArray();
                if (m_prepared[m] != null)
                {
                    // cached BVH from the prepared solid; its slack policy
                    // covers eps-welding position shifts, the guard rebuilds
                    // when the scene demands more
                    m_bvh[m] = m_prepared[m]!.Bvh(2 * slackScene);
                    continue;
                }
                var boxes = new Box3d[list.Count];
                for (var i = 0; i < list.Count; i++)
                {
                    var t = list[i];
                    var p0 = m_kernel.Positions[m_kernel.T0[t]];
                    var p1 = m_kernel.Positions[m_kernel.T1[t]];
                    var p2 = m_kernel.Positions[m_kernel.T2[t]];
                    boxes[i] = new Box3d(p0, p1, p2).EnlargedBy(slackScene);
                }
                m_bvh[m] = new CsgBvh(boxes);
            }
            var pairs = new List<(int, int)>();
            for (var i = 0; i < n; i++)
                for (var j = i + 1; j < n; j++)
                {
                    if (!m_bvh[i].RootBox.Intersects(m_bvh[j].RootBox)) continue;
                    var ti = m_triOf[i]; var tj = m_triOf[j];
                    m_bvh[i].ForEachIntersectingPair(m_bvh[j], (x, y) => pairs.Add((ti[x], tj[y])));
                }
            return pairs;
        }

        #endregion

        #region narrow phase: intersection segments

        private readonly struct CrossPt
        {
            public readonly int Vid;            // existing vertex, or -1
            public readonly int EdgeA, EdgeB;   // edge to cut (canonical ids) when Vid < 0
            public readonly V3d Pos;
            public readonly double Factor;      // adaptive tolerance factor (conditioning of the cut)
            public CrossPt(int vid, V3d pos, double factor) { Vid = vid; EdgeA = EdgeB = -1; Pos = pos; Factor = factor; }
            public CrossPt(int ea, int eb, V3d pos, double factor) { Vid = -1; EdgeA = ea; EdgeB = eb; Pos = pos; Factor = factor; }
        }

        private enum PairKind : byte { None, Coplanar, Segment }

        private readonly struct PairResult
        {
            public readonly PairKind Kind;
            public readonly bool CoplanarSame;
            public readonly CrossPt Lo, Hi;
            public readonly int LoCutPlane, HiCutPlane;
            public PairResult(bool same) { Kind = PairKind.Coplanar; CoplanarSame = same; Lo = Hi = default; LoCutPlane = HiCutPlane = 0; }
            public PairResult(CrossPt lo, int loPlane, CrossPt hi, int hiPlane)
            { Kind = PairKind.Segment; CoplanarSame = false; Lo = lo; LoCutPlane = loPlane; Hi = hi; HiCutPlane = hiPlane; }
        }

        /// <summary>
        /// Pure (read-only, deterministic) part of a pair: signs, coplanarity,
        /// interval overlap. HeightSign is a pure function, so recomputing
        /// per pair is exactly as consistent as the former shared cache.
        /// </summary>
        private PairResult ComputePair(int ta, int tb)
        {
            var pa = m_kernel.TriPlane[ta];
            var pb = m_kernel.TriPlane[tb];

            Span<int> va = stackalloc int[] { m_kernel.T0[ta], m_kernel.T1[ta], m_kernel.T2[ta] };
            Span<int> vb = stackalloc int[] { m_kernel.T0[tb], m_kernel.T1[tb], m_kernel.T2[tb] };
            Span<Sign3> sb = stackalloc Sign3[3];
            Span<Sign3> sa = stackalloc Sign3[3];
            for (var i = 0; i < 3; i++) sb[i] = Sign(pa, vb[i]);
            if (AllStrict(sb, Sign3.Above) || AllStrict(sb, Sign3.Below)) return default;
            for (var i = 0; i < 3; i++) sa[i] = Sign(pb, va[i]);
            if (AllStrict(sa, Sign3.Above) || AllStrict(sa, Sign3.Below)) return default;

            if (AllOn(sa) && AllOn(sb))
            {
                if (!CoplanarInteriorsOverlap(va, vb, pa)) return default;
                return new PairResult(m_kernel.Planes[pa].Normal.Dot(m_kernel.Planes[pb].Normal) > 0);
            }

            var crossA = CrossingPoints(va, sa, pb);
            var crossB = CrossingPoints(vb, sb, pa);
            if (crossA.Count < 2 || crossB.Count < 2) return default;


            var dir = m_kernel.Planes[pa].Normal.Cross(m_kernel.Planes[pb].Normal);
            var (loA, hiA) = Interval(crossA, dir);
            var (loB, hiB) = Interval(crossB, dir);
            var (lo, loCutPlane) = loA.T > loB.T ? (loA, pb) : (loB, pa);
            var (hi, hiCutPlane) = hiA.T < hiB.T ? (hiA, pb) : (hiB, pa);
            if (lo.T >= hi.T) return default;
            if (m_eps.AreCoincident(lo.P.Pos, hi.P.Pos, lo.P.Factor.Max(hi.P.Factor))) return default;
            return new PairResult(lo.P, loCutPlane, hi.P, hiCutPlane);
        }

        /// <summary>Order-dependent part: vertex ids, welding, registration.</summary>
        private void MaterializePair(int ta, int tb, in PairResult r)
        {
            switch (r.Kind)
            {
                case PairKind.None: return;
                case PairKind.Coplanar:
                    m_coplanar.GetOrCreate(ta, _ => new List<(int, bool)>()).Add((tb, r.CoplanarSame));
                    m_coplanar.GetOrCreate(tb, _ => new List<(int, bool)>()).Add((ta, r.CoplanarSame));
                    return;
                case PairKind.Segment:
                    var v0 = Materialize(r.Lo, r.LoCutPlane);
                    var v1 = Materialize(r.Hi, r.HiCutPlane);
                    if (v0 == v1) return;
                    AddConstraint(ta, v0, v1);
                    AddConstraint(tb, v0, v1);
                    return;
                default: throw new InvalidOperationException();
            }
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
                        result.Add(new CrossPt(vi, m_kernel.Positions[vi], m_kernel.TolFactor[vi]));
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
                    // conditioning: position error along the edge ~ eps·L/|Δh|;
                    // grazing cuts get a wide (capped) tolerance factor so
                    // consistency welding still finds them
                    var l = (pj - pi).NormMax;
                    // first-order cut position error: L·(|hi|+|hj|)/Δh² times
                    // the per-coordinate slop — small for endpoint-hugging
                    // perpendicular cuts, large for genuine grazing
                    var dh = hi - hj;
                    var conditioning = (2 * Eps.GenerationFactor * l * (hi.Abs() + hj.Abs()) / (dh * dh))
                        .Clamp(Eps.GenerationFactor, Eps.MaxFactor);
                    result.Add(new CrossPt(v[i], v[j], pi + t * (pj - pi), conditioning));
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

            if (m_eps.AreCoincident(c.Pos, m_kernel.Positions[c.EdgeA], c.Factor)) vid = c.EdgeA;
            else if (m_eps.AreCoincident(c.Pos, m_kernel.Positions[c.EdgeB], c.Factor)) vid = c.EdgeB;
            else
            {
                // triple-plane corners are reached via several distinct
                // (edge, plane) cuts: weld onto a coincident existing vertex
                vid = GridFindCoincident(c.Pos, c.Factor);
                if (vid < 0)
                {
                    vid = m_kernel.Positions.Count;
                    m_kernel.Positions.Add(c.Pos);
                    m_kernel.TolFactor.Add(c.Factor);
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

        internal static long EdgeKey(int a, int b)
            => a < b ? ((long)a << 32) | (uint)b : ((long)b << 32) | (uint)a;

        private void AddConstraint(int tri, int v0, int v1)
            => m_faceConstraints.GetOrCreate(tri, _ => new HashSet<(int, int)>()).Add(SortedEdge(v0, v1));

        #endregion

        #region subdivision

        private void Subdivide()
        {
            SplitConstraints();

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
                            var tjf = Fun.Max(m_kernel.TolFactor[e], Eps.GenerationFactor);
                            if (m_eps.AreaSign(p[i], p[j], q, tjf) != Sign3.On) continue;
                            var d = p[j] - p[i]; var w = q - p[i];
                            var dot = d.Dot(w);
                            if (dot <= 0 || dot >= d.LengthSquared) continue;
                            RegisterEdgePoint(v[i], v[j], e);
                        }
                    }
                }
            }

            // per-face triangulations run in parallel (read-only kernel,
            // private CDT state); assembly stays in face order → deterministic
            var results = new (List<(int, int, int)> Tris, List<(int, int)> Constraints)?[m_kernel.TriangleCount];
            Lap2("subdiv-prep");
            CsgParallel.For(0, m_kernel.TriangleCount, m_maxThreads, tri =>
            {
                var constraints = m_faceConstraints.GetOrDefault(tri);
                var boundary = BoundaryPoints(tri);
                if (constraints == null && boundary == null) return;

                m_diagTri = tri;
                var plane = m_kernel.Planes[m_kernel.TriPlane[tri]];
                V2d Proj(int vid) => Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[vid]);

                // face-level adaptive tolerance: the widest factor among the
                // points this face must integrate (grazing cuts widen it)
                var faceFactor = Eps.GenerationFactor;
                if (constraints != null)
                    foreach (var (ca, cb) in constraints)
                        faceFactor = faceFactor.Max(m_kernel.TolFactor[ca]).Max(m_kernel.TolFactor[cb]);
                if (boundary != null)
                    foreach (var vid in boundary) faceFactor = faceFactor.Max(m_kernel.TolFactor[vid]);

                var cdt = FaceCdt.Rent(m_eps, faceFactor,
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

                try
                {
                    results[tri] = cdt.Triangulate();
                }
                catch (CsgVerificationException e)
                {
                    throw new CsgVerificationException(Diag(e));
                }
            });

            Lap2("subdiv-cdt");
            var counts = new int[m_kernel.TriangleCount + 1];
            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
                counts[tri + 1] = counts[tri] + (results[tri] == null ? 1 : results[tri]!.Value.Tris.Count);
            var fragmentArray = new Fragment[counts[m_kernel.TriangleCount]];
            CsgParallel.For(0, m_kernel.TriangleCount, m_maxThreads, tri =>
            {
                var at = counts[tri];
                if (results[tri] == null)
                {
                    fragmentArray[at] = new Fragment(m_kernel.T0[tri], m_kernel.T1[tri], m_kernel.T2[tri], tri);
                    return;
                }
                foreach (var (a, b, c) in results[tri]!.Value.Tris)
                    fragmentArray[at++] = new Fragment(a, b, c, tri);
            });
            Fragments.Clear();
            Fragments.AddRange(fragmentArray);
            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
            {
                if (results[tri] == null) continue;
                var barrier = m_barriers[m_kernel.TriMesh[tri]];
                foreach (var (a, b) in results[tri]!.Value.Constraints) barrier.Add(EdgeKey(a, b));
            }
        }

        private string Diag(Exception e, params int[] vids)
        {
            var tri = m_diagTri;
            var msg = $"{e.Message} [tri {tri} mesh {m_kernel.TriMesh[tri]} face {m_kernel.TriFace[tri]} " +
                $"corners ({m_kernel.T0[tri]}:{m_kernel.Positions[m_kernel.T0[tri]]}, {m_kernel.T1[tri]}:{m_kernel.Positions[m_kernel.T1[tri]]}, {m_kernel.T2[tri]}:{m_kernel.Positions[m_kernel.T2[tri]]})";
            foreach (var v in vids) msg += $" point {v}:{m_kernel.Positions[v]} f {m_kernel.TolFactor[v]:0.#}";
            return msg + "]";
        }

        [ThreadStatic]
        private static int m_diagTri;

        /// <summary>
        /// Resolves interactions between constraint segments before
        /// triangulation. Two effects, both recorded in a GLOBAL per-segment
        /// split map and applied to every face carrying the segment (a face
        /// pair shares each segment, and with three or more solids a vertex on
        /// a segment can be known to only one of its faces):
        /// (1) proper crossings of two segments in a face (three-plane points,
        ///     generation-2, welded through the vertex grid);
        /// (2) vertices of a face's point pool (constraint endpoints and
        ///     boundary points) lying on a segment's interior.
        /// </summary>
        private void SplitConstraints()
        {
            var splits = new Dictionary<(int, int), HashSet<int>>();
            void AddSplit((int, int) seg, int vid)
                => splits.GetOrCreate(seg, _ => new HashSet<int>()).Add(vid);

            foreach (var (tri, constraints) in m_faceConstraints)
            {
                var normal = m_kernel.Planes[m_kernel.TriPlane[tri]].Normal;
                var segs = new List<(int, int)>(constraints).ToArray();

                // (1) proper crossings within this face
                if (m_kernel.MeshCount > 2)
                {
                    for (var i = 0; i < segs.Length; i++)
                    {
                        var (a, b) = segs[i];
                        var a2 = Triangulator.ProjectDominant(normal, m_kernel.Positions[a]);
                        var b2 = Triangulator.ProjectDominant(normal, m_kernel.Positions[b]);
                        for (var j = i + 1; j < segs.Length; j++)
                        {
                            var (c, d) = segs[j];
                            if (a == c || a == d || b == c || b == d) continue;
                            var c2 = Triangulator.ProjectDominant(normal, m_kernel.Positions[c]);
                            var d2 = Triangulator.ProjectDominant(normal, m_kernel.Positions[d]);
                            var xf = Fun.Max(
                                Fun.Max(m_kernel.TolFactor[a], m_kernel.TolFactor[b]),
                                Fun.Max(m_kernel.TolFactor[c], m_kernel.TolFactor[d])).Max(Eps.GenerationFactor);
                            var sc = m_eps.AreaSign(a2, b2, c2, xf);
                            var sd = m_eps.AreaSign(a2, b2, d2, xf);
                            if (!((sc == Sign3.Above && sd == Sign3.Below) || (sc == Sign3.Below && sd == Sign3.Above))) continue;
                            var sa = m_eps.AreaSign(c2, d2, a2, xf);
                            var sb = m_eps.AreaSign(c2, d2, b2, xf);
                            if (!((sa == Sign3.Above && sb == Sign3.Below) || (sa == Sign3.Below && sb == Sign3.Above))) continue;

                            var num = Det(c2 - a2, d2 - c2);
                            var den = Det(b2 - a2, d2 - c2);
                            if (den == 0.0) continue;
                            var t = num / den;
                            var p = m_kernel.Positions[a] + t.Clamp(0, 1) * (m_kernel.Positions[b] - m_kernel.Positions[a]);
                            var factor = (Eps.GenerationFactor * Fun.Max(
                                Fun.Max(m_kernel.TolFactor[a], m_kernel.TolFactor[b]),
                                Fun.Max(m_kernel.TolFactor[c], m_kernel.TolFactor[d])))
                                .Clamp(Eps.GenerationFactor, Eps.MaxFactor);
                            var vid = GridFindCoincident(p, factor);
                            if (vid < 0)
                            {
                                vid = m_kernel.Positions.Count;
                                m_kernel.Positions.Add(p);
                                m_kernel.TolFactor.Add(factor);
                                GridAdd(vid);
                            }
                            AddSplit(segs[i], vid);
                            AddSplit(segs[j], vid);
                        }
                    }
                }

                // (2) pool vertices on segment interiors
                var pool = new HashSet<int>();
                foreach (var (a, b) in segs) { pool.Add(a); pool.Add(b); }
                var boundary = BoundaryPoints(tri);
                if (boundary != null) foreach (var v in boundary) pool.Add(v);

                foreach (var seg in segs)
                {
                    var (a, b) = seg;
                    var a2 = Triangulator.ProjectDominant(normal, m_kernel.Positions[a]);
                    var b2 = Triangulator.ProjectDominant(normal, m_kernel.Positions[b]);
                    var d2 = b2 - a2;
                    var len2 = d2.LengthSquared;
                    foreach (var v in pool)
                    {
                        if (v == a || v == b) continue;
                        var v2 = Triangulator.ProjectDominant(normal, m_kernel.Positions[v]);
                        var pf = Fun.Max(Fun.Max(m_kernel.TolFactor[a], m_kernel.TolFactor[b]),
                            m_kernel.TolFactor[v]).Max(Eps.GenerationFactor);
                        if (m_eps.AreaSign(a2, b2, v2, pf) != Sign3.On) continue;
                        var t = d2.Dot(v2 - a2);
                        if (t <= 0 || t >= len2) continue;
                        AddSplit(seg, v);
                    }
                }
            }
            if (splits.Count == 0) return;

            // apply the global split map in every face carrying a split segment
            foreach (var (_, constraints) in m_faceConstraints)
            {
                var segs = new List<(int, int)>(constraints).ToArray();
                foreach (var seg in segs)
                {
                    if (!splits.TryGetValue(seg, out var points)) continue;
                    constraints.Remove(seg);
                    var (a, b) = seg;
                    var dir = m_kernel.Positions[b] - m_kernel.Positions[a];
                    var sorted = new List<int>(points);
                    sorted.Sort((x, y) => dir.Dot(m_kernel.Positions[x] - m_kernel.Positions[a])
                        .CompareTo(dir.Dot(m_kernel.Positions[y] - m_kernel.Positions[a])));
                    var prev = a;
                    foreach (var v in sorted)
                    {
                        if (v != prev) constraints.Add(SortedEdge(prev, v));
                        prev = v;
                    }
                    if (prev != b) constraints.Add(SortedEdge(prev, b));
                }
            }

            static double Det(V2d u, V2d v) => u.X * v.Y - u.Y * v.X;
        }

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
            var n = m_kernel.MeshCount;
            m_rel = new byte[Fragments.Count * n]; // FragLabel.Outside

            // coplanar-covered fragments are labeled directly per partner mesh
            // (their coverage boundary is made of constraint edges, so regions
            // are uniform with respect to every other mesh)
            for (var f = 0; f < Fragments.Count; f++)
            {
                var frag = Fragments[f];
                var partners = m_coplanar.GetOrDefault(frag.Parent);
                if (partners == null) continue;
                var centroid = (m_kernel.Positions[frag.V0] + m_kernel.Positions[frag.V1] + m_kernel.Positions[frag.V2]) / 3.0;
                foreach (var (partner, same) in partners)
                {
                    var pm = m_kernel.TriMesh[partner];
                    if (m_rel[f * n + pm] != 0) continue;
                    if (!CoplanarCovers(partner, centroid)) continue;
                    m_rel[f * n + pm] = (byte)(same ? FragLabel.OnSame : FragLabel.OnOpposite);
                }
            }

            // fragment adjacency across shared (canonical) sub-edges via one sort
            var keys = new long[Fragments.Count * 3];
            var frags = new int[Fragments.Count * 3];
            var fragmentCount = Fragments.Count;
            CsgParallel.For(0, fragmentCount, m_maxThreads, f =>
            {
                var frag = Fragments[f];
                keys[f * 3] = EdgeKey(frag.V0, frag.V1);
                keys[f * 3 + 1] = EdgeKey(frag.V1, frag.V2);
                keys[f * 3 + 2] = EdgeKey(frag.V2, frag.V0);
                frags[f * 3] = f; frags[f * 3 + 1] = f; frags[f * 3 + 2] = f;
            });
            Lap2("classify-keys");
            RadixSorter.SortEdgeKeys(keys, frags, keys.Length, m_maxThreads);
            Lap2("classify-radix");
            // CSR adjacency: run-aligned parallel blocks; counts and fill use
            // atomic cursors (neighbor ORDER within a fragment is irrelevant —
            // region sets and label values are order-independent)
            var nbrCount = new int[Fragments.Count];
            var blockStarts = RunAlignedBlocks(keys, m_maxThreads);
            for (var pass = 0; pass < 2; pass++)
            {
                int[]? nbr = null;
                int[]? offsets = null;
                if (pass == 1)
                {
                    offsets = new int[Fragments.Count + 1];
                    for (var f = 0; f < Fragments.Count; f++) offsets[f + 1] = offsets[f] + nbrCount[f];
                    nbr = new int[offsets[Fragments.Count]];
                    Array.Clear(nbrCount, 0, nbrCount.Length);
                }
                var p = pass;
                CsgParallel.For(0, blockStarts.Length - 1, m_maxThreads, blk =>
                {
                    var lo = blockStarts[blk];
                    var hi = blockStarts[blk + 1];
                    for (var i = lo; i < hi;)
                    {
                        var j = i + 1;
                        while (j < hi && keys[j] == keys[i]) j++;
                        for (var x = i; x < j; x++)
                            for (var y = x + 1; y < j; y++)
                            {
                                var fx = frags[x]; var fy = frags[y];
                                var mesh = m_kernel.TriMesh[Fragments[fx].Parent];
                                if (m_kernel.TriMesh[Fragments[fy].Parent] != mesh) continue;
                                if (m_barriers[mesh].Contains(keys[i])) continue;
                                if (p == 0)
                                {
                                    System.Threading.Interlocked.Increment(ref nbrCount[fx]);
                                    System.Threading.Interlocked.Increment(ref nbrCount[fy]);
                                }
                                else
                                {
                                    nbr![offsets![fx] + System.Threading.Interlocked.Increment(ref nbrCount[fx]) - 1] = fy;
                                    nbr[offsets[fy] + System.Threading.Interlocked.Increment(ref nbrCount[fy]) - 1] = fx;
                                }
                            }
                        i = j;
                    }
                });
                if (pass == 1) { m_adjNbr = nbr!; m_adjOffsets = offsets!; }
            }

            Lap2("classify-csr");
            var visited = new bool[Fragments.Count];
            var regions = new List<List<int>>();
            for (var seedFrag = 0; seedFrag < Fragments.Count; seedFrag++)
            {
                if (visited[seedFrag]) continue;
                var region = new List<int>();
                var stack = new Stack<int>();
                stack.Push(seedFrag);
                visited[seedFrag] = true;
                while (stack.Count > 0)
                {
                    var f = stack.Pop();
                    region.Add(f);
                    for (var e = m_adjOffsets[f]; e < m_adjOffsets[f + 1]; e++)
                    {
                        var g = m_adjNbr[e];
                        if (visited[g]) continue;
                        visited[g] = true;
                        stack.Push(g);
                    }
                }
                regions.Add(region);
            }

            Lap2("classify-flood");
            // per (region × other mesh) classification is independent: reads
            // the kernel and BVHs, writes disjoint label slots
            CsgParallel.For(0, regions.Count, m_maxThreads, ri =>
            {
                var region = regions[ri];
                var mi = m_kernel.TriMesh[Fragments[region[0]].Parent];
                for (var m = 0; m < n; m++)
                {
                    if (m == mi) continue;
                    byte pre = 0;
                    foreach (var f in region)
                    {
                        if (m_rel[f * n + m] == 0) continue;
                        pre = m_rel[f * n + m];
                        break;
                    }
                    if (pre == 0)
                        pre = (byte)(RegionIsInsideOther(region, m) ? FragLabel.Inside : FragLabel.Outside);
                    foreach (var f in region)
                        if (m_rel[f * n + m] == 0) m_rel[f * n + m] = pre;
                }
            });
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
            return m_eps.AreaSign(t[0], t[1], q, Eps.GenerationFactor) != Sign3.Below
                && m_eps.AreaSign(t[1], t[2], q, Eps.GenerationFactor) != Sign3.Below
                && m_eps.AreaSign(t[2], t[0], q, Eps.GenerationFactor) != Sign3.Below;
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

        /// <summary>Parity of ray/other-mesh crossings; null when any hit is eps-ambiguous. BVH-accelerated.</summary>
        private bool? RayParity(V3d o, V3d dir, int otherMesh)
        {
            var count = 0;
            foreach (var t in m_bvh[otherMesh].RayCandidates(o, dir, m_triOf[otherMesh]))
            {
                var plane = m_kernel.Planes[m_kernel.TriPlane[t]];
                var denom = plane.Normal.Dot(dir);
                var h = plane.Normal.Dot(o) - plane.Distance;
                if (denom.Abs() < 1e-9)
                {
                    if (m_eps.HeightSign(plane, o, Eps.GenerationFactor) == Sign3.On) return null; // ray (nearly) in plane near origin
                    continue;
                }
                var s = -h / denom;
                if (s <= 0) continue;
                var hit = o + s * dir;
                if (m_eps.AreCoincident(hit, o, Eps.GenerationFactor)) return null; // origin on the other surface

                var p0 = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[m_kernel.T0[t]]);
                var p1 = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[m_kernel.T1[t]]);
                var p2 = Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[m_kernel.T2[t]]);
                var q = Triangulator.ProjectDominant(plane.Normal, hit);
                var s0 = m_eps.AreaSign(p0, p1, q, Eps.GenerationFactor);
                var s1 = m_eps.AreaSign(p1, p2, q, Eps.GenerationFactor);
                var s2 = m_eps.AreaSign(p2, p0, q, Eps.GenerationFactor);
                if (s0 == Sign3.Below || s1 == Sign3.Below || s2 == Sign3.Below) continue; // outside triangle
                if (s0 == Sign3.On || s1 == Sign3.On || s2 == Sign3.On) return null;       // grazing edge/vertex
                count++;
            }
            return (count & 1) == 1;
        }

        #endregion
    }
}
