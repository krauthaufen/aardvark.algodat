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
        private readonly Dictionary<int, List<(int Partner, bool Same, double Factor)>> m_coplanar = new(); // tri -> overlapping coplanar tris of the other mesh
        private CsgBvh[] m_bvh = Array.Empty<CsgBvh>();
        private int[][] m_triOf = Array.Empty<int[]>();
        private int[] m_adjNbr = Array.Empty<int>();
        private int[] m_adjOffsets = Array.Empty<int>();

        /// <summary>optional pre-built solids providing cached BVHs (index-aligned with meshes; entries may be null)</summary>
        private readonly CsgMesh?[] m_prepared;

        private readonly int m_maxThreads;

        public Pipeline(Kernel kernel, CsgMesh?[]? prepared = null, int maxThreads = 1, bool selfResolve = false)
        {
            m_maxThreads = maxThreads.Max(1);
            m_selfResolve = selfResolve;
            kernel.UpdateSceneScale();
            m_kernel = kernel;
            m_eps = kernel.Eps;
            m_prepared = prepared ?? new CsgMesh?[kernel.MeshCount];
            m_barriers = new HashSet<long>[kernel.MeshCount]
                .SetByIndex(_ => new HashSet<long>(MixedLongComparer.Instance));
        }

        /// <summary>
        /// Self-resolution mode: instead of classifying fragments against other
        /// meshes, the single ingested mesh is arranged against ITSELF and the
        /// boundary of its positive-winding region is selected. Fills
        /// <see cref="SelfKeep"/> / <see cref="SelfFlip"/> in place of the
        /// cross-mesh label table.
        /// </summary>
        private readonly bool m_selfResolve;

        /// <summary>self-resolve: whether each fragment is on the union (winding≥1) boundary.</summary>
        public bool[] SelfKeep = Array.Empty<bool>();
        /// <summary>self-resolve: whether the kept fragment's winding is reversed relative to its parent.</summary>
        public bool[] SelfFlip = Array.Empty<bool>();

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
            WeldPlanes(pairs);
            Lap("planeweld");
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
            if (m_selfResolve) SelfClassify(); else Classify();
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
                            if (m_eps.AreCoincident(p, m_kernel.Positions[j], factor)) return RepLate(j);
                    }
            return -1;
        }

        #endregion

        #region plane welding

        /// <summary>plane id → welded group id</summary>
        private int[] m_planeGroup = Array.Empty<int>();

        /// <summary>
        /// Globally welds near-coincident planes (canonicalize first, decide
        /// later): candidate plane pairs come from the broad-phase triangle
        /// pairs; two planes weld when all six vertices lie within the
        /// PlaneWeldFactor tolerance slab of the other plane. Welded planes
        /// snap to the group representative's geometry (orientation-aligned
        /// per face), so every affected pair becomes exactly coplanar and is
        /// handled by the coplanar machinery — consistently across all faces,
        /// which per-pair escalation cannot guarantee.
        /// </summary>
        private void WeldPlanes(List<(int, int)> pairs)
        {
            var n = m_kernel.Planes.Count;
            var parent = new int[n].SetByIndex(i => i);
            int Find(int i) { while (parent[i] != i) { parent[i] = parent[parent[i]]; i = parent[i]; } return i; }

            foreach (var (ta, tb) in pairs)
            {
                var pa = m_kernel.TriPlane[ta];
                var pb = m_kernel.TriPlane[tb];
                var ra = Find(pa); var rb = Find(pb);
                if (ra == rb) continue;

                var qa = m_kernel.Planes[pa];
                var qb = m_kernel.Planes[pb];
                // face-relative criterion: separation ≤ K·L (K tied to the
                // conditioning cap so faces are either weldable or their cuts
                // resolvable) plus the absolute eps floor — NOT scene-relative
                // (a scene-relative slab would weld distinct planes at large
                // offsets)
                Span<int> wa = stackalloc int[] { m_kernel.T0[ta], m_kernel.T1[ta], m_kernel.T2[ta] };
                Span<int> wb = stackalloc int[] { m_kernel.T0[tb], m_kernel.T1[tb], m_kernel.T2[tb] };
                var l = 0.0;
                var mag = 0.0;
                for (var i = 0; i < 3; i++)
                {
                    var qai = m_kernel.Positions[wa[i]];
                    var qbi = m_kernel.Positions[wb[i]];
                    l = l.Max((qai - m_kernel.Positions[wa[(i + 1) % 3]]).NormMax)
                         .Max((qbi - m_kernel.Positions[wb[(i + 1) % 3]]).NormMax);
                    mag = mag.Max(qai.NormMax).Max(qbi.NormMax);
                }
                var limit = 8.0 * l / Eps.MaxFactor
                    + Eps.GenerationFactor * m_eps.Relative * (mag + m_eps.Scene);
                var weld = true;
                for (var i = 0; i < 3 && weld; i++)
                {
                    weld = (qb.Normal.Dot(m_kernel.Positions[wa[i]]) - qb.Distance).Abs() <= limit
                        && (qa.Normal.Dot(m_kernel.Positions[wb[i]]) - qa.Distance).Abs() <= limit;
                }
                if (!weld) continue;

                if (ra < rb) parent[rb] = ra; else parent[ra] = rb;
            }

            m_planeGroup = new int[n];
            for (var i = 0; i < n; i++)
            {
                var r = Find(i);
                m_planeGroup[i] = r;
                if (r == i) continue;
                // snap to the representative's geometry, orientation-aligned
                var canon = m_kernel.Planes[r];
                m_kernel.Planes[i] = m_kernel.Planes[i].Normal.Dot(canon.Normal) >= 0
                    ? canon
                    : new Plane3d(-canon.Normal, -canon.Distance);
            }
        }

        /// <summary>Point inside-or-on the triangle's 2D projection, at the point's tolerance factor.</summary>
        private bool WithinFace(int tri, int vid)
        {
            var normal = m_kernel.Planes[m_kernel.TriPlane[tri]].Normal;
            var q = Triangulator.ProjectDominant(normal, m_kernel.Positions[vid]);
            Span<V2d> t = stackalloc V2d[3];
            t[0] = Triangulator.ProjectDominant(normal, m_kernel.Positions[m_kernel.T0[tri]]);
            t[1] = Triangulator.ProjectDominant(normal, m_kernel.Positions[m_kernel.T1[tri]]);
            t[2] = Triangulator.ProjectDominant(normal, m_kernel.Positions[m_kernel.T2[tri]]);
            var det = (t[1] - t[0]).X * (t[2] - t[0]).Y - (t[1] - t[0]).Y * (t[2] - t[0]).X;
            if (det == 0) return false;
            if (det < 0) (t[1], t[2]) = (t[2], t[1]);
            var f = m_kernel.TolFactor[vid].Max(Eps.GenerationFactor);
            return m_eps.AreaSign(t[0], t[1], q, f) != Sign3.Below
                && m_eps.AreaSign(t[1], t[2], q, f) != Sign3.Below
                && m_eps.AreaSign(t[2], t[0], q, f) != Sign3.Below;
        }

        /// <summary>
        /// Makes sure vid is insertable into tri's face CDT: if it lies
        /// strictly outside an edge at its current factor (clip endpoints can
        /// land marginally beyond a corner), the factor is widened to the
        /// measured outside distance with GenerationFactor margin. Returns
        /// false when even MaxFactor cannot absorb the offset — then the
        /// point is genuinely outside the face.
        /// </summary>
        private bool EnsureInsertable(int tri, int vid)
        {
            var normal = m_kernel.Planes[m_kernel.TriPlane[tri]].Normal;
            var q = Triangulator.ProjectDominant(normal, m_kernel.Positions[vid]);
            Span<V2d> t = stackalloc V2d[3];
            t[0] = Triangulator.ProjectDominant(normal, m_kernel.Positions[m_kernel.T0[tri]]);
            t[1] = Triangulator.ProjectDominant(normal, m_kernel.Positions[m_kernel.T1[tri]]);
            t[2] = Triangulator.ProjectDominant(normal, m_kernel.Positions[m_kernel.T2[tri]]);
            var det = (t[1] - t[0]).X * (t[2] - t[0]).Y - (t[1] - t[0]).Y * (t[2] - t[0]).X;
            if (det == 0) return false;
            if (det < 0) (t[1], t[2]) = (t[2], t[1]);

            var needed = Eps.GenerationFactor;
            for (var i = 0; i < 3; i++)
            {
                var a = t[i];
                var b = t[(i + 1) % 3];
                var d1 = b - a;
                var d2 = q - a;
                var d = d1.X * d2.Y - d1.Y * d2.X;
                if (d >= 0) continue;
                // factor that turns this strict Below into On (AreaSign tol
                // model: eps * (m + l + Scene) * l * factor)
                var m = Fun.Max(a.X.Abs(), a.Y.Abs(), b.X.Abs(), b.Y.Abs()).Max(
                        Fun.Max(q.X.Abs(), q.Y.Abs()));
                var l = Fun.Max(d1.X.Abs(), d1.Y.Abs(), d2.X.Abs(), d2.Y.Abs());
                var denom = m_eps.Relative * (m + l + m_eps.Scene) * l;
                if (denom <= 0) return false;
                needed = needed.Max(Eps.GenerationFactor * -d / denom);
            }
            if (needed > Eps.MaxFactor) return false;
            if (needed > m_kernel.TolFactor[vid]) BumpFactor(vid, needed);
            return true;
        }

        /// <summary>Geometric winding normal of a kernel triangle (not normalized).</summary>
        private V3d WindingNormal(int t)
        {
            var p0 = m_kernel.Positions[m_kernel.T0[t]];
            return (m_kernel.Positions[m_kernel.T1[t]] - p0).Cross(m_kernel.Positions[m_kernel.T2[t]] - p0);
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
            // the broad phase must be conservative against the WIDEST
            // tolerance any later pass can apply (adaptive factors reach
            // MaxFactor): grazing twin faces separated by less than the
            // MaxFactor coincidence radius still interact — with a smaller
            // slack, axis-planar twins (zero box extent) are never paired,
            // so coplanar sheets go unregistered and selection cracks
            var slackScene = 4 * Eps.MaxFactor * m_eps.Relative * (m_eps.Scene + 1e-300);
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
            if (s_debugPair != null)
            {
                var parts = s_debugPair.Split(',');
                foreach (var ts in parts)
                {
                    var t = int.Parse(ts);
                    Console.WriteLine($"PREWELD tri {t} mesh {m_kernel.TriMesh[t]} face {m_kernel.TriFace[t]}: " +
                        $"{m_kernel.T0[t]}@{m_kernel.Positions[m_kernel.T0[t]]} {m_kernel.T1[t]}@{m_kernel.Positions[m_kernel.T1[t]]} " +
                        $"{m_kernel.T2[t]}@{m_kernel.Positions[m_kernel.T2[t]]}");
                }
            }
            var pairs = new List<(int, int)>();
            for (var i = 0; i < n; i++)
                for (var j = i + 1; j < n; j++)
                {
                    if (!m_bvh[i].RootBox.Intersects(m_bvh[j].RootBox)) continue;
                    var ti = m_triOf[i]; var tj = m_triOf[j];
                    m_bvh[i].ForEachIntersectingPair(m_bvh[j], (x, y) =>
                    {
                        if (s_debugPair != null && s_debugPair == $"{ti[x]},{tj[y]}")
                            Console.WriteLine($"BROADPAIR {ti[x]},{tj[y]}");
                        pairs.Add((ti[x], tj[y]));
                    });
                }
            if (m_selfResolve)
            {
                // intra-mesh pairs for self-intersection: unordered, and skip
                // topologically adjacent triangles (sharing a welded vertex),
                // which merely meet along a shared edge and never self-cross
                var seen = new HashSet<long>(MixedLongComparer.Instance);
                for (var m = 0; m < n; m++)
                {
                    var tm = m_triOf[m];
                    m_bvh[m].ForEachIntersectingPair(m_bvh[m], (x, y) =>
                    {
                        var a = tm[x]; var b = tm[y];
                        if (a >= b) return;
                        if (ShareVertex(a, b)) return;
                        if (seen.Add(((long)a << 32) | (uint)b)) pairs.Add((a, b));
                    });
                }
            }
            if (Environment.GetEnvironmentVariable("CSG_BRUTE") != null)
            {
                Box3d TriBox(int t) => new Box3d(
                    m_kernel.Positions[m_kernel.T0[t]], m_kernel.Positions[m_kernel.T1[t]],
                    m_kernel.Positions[m_kernel.T2[t]]).EnlargedBy(slackScene);
                var set = new HashSet<(int, int)>(pairs);
                var missing = 0;
                for (var i = 0; i < n; i++)
                    for (var j = i + 1; j < n; j++)
                        foreach (var ta in m_triOf[i])
                        {
                            var ba = TriBox(ta);
                            foreach (var tb in m_triOf[j])
                                if (ba.Intersects(TriBox(tb)) && !set.Contains((ta, tb)) && missing++ < 8)
                                    Console.WriteLine($"BRUTE missing pair {ta},{tb}");
                        }
                Console.WriteLine($"BRUTE {missing} missing of {pairs.Count} found");
            }
            return pairs;
        }

        private bool ShareVertex(int a, int b)
        {
            int a0 = m_kernel.T0[a], a1 = m_kernel.T1[a], a2 = m_kernel.T2[a];
            int b0 = m_kernel.T0[b], b1 = m_kernel.T1[b], b2 = m_kernel.T2[b];
            return a0 == b0 || a0 == b1 || a0 == b2 || a1 == b0 || a1 == b1 || a1 == b2 || a2 == b0 || a2 == b1 || a2 == b2;
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
            public readonly double CoplanarFactor;   // measured slab of the coplanar pair (8 = exact)
            public readonly CrossPt Lo, Hi;
            public readonly int LoCutPlane, HiCutPlane;
            public PairResult(bool same, double coplanarFactor)
            { Kind = PairKind.Coplanar; CoplanarSame = same; CoplanarFactor = coplanarFactor; Lo = Hi = default; LoCutPlane = HiCutPlane = 0; }
            public PairResult(CrossPt lo, int loPlane, CrossPt hi, int hiPlane)
            { Kind = PairKind.Segment; CoplanarSame = false; CoplanarFactor = 0; Lo = lo; LoCutPlane = loPlane; Hi = hi; HiCutPlane = hiPlane; }
        }

        /// <summary>
        /// Measured tolerance factor of a coplanar pair: the widest mutual
        /// vertex height, in eps units — computed from the pair's OWN geometry
        /// (winding-normal planes), independent of group snapping, so exactly
        /// coplanar pairs measure exact even inside a larger welded group.
        /// </summary>
        private double CoplanarSlabFactor(Span<int> va, Span<int> vb, int pa)
        {
            var na = WindingNormal(0, va).Normalized;
            var da = na.Dot(m_kernel.Positions[va[0]]);
            var planeA = new Plane3d(na, da);
            var nb = WindingNormal(0, vb).Normalized;
            var db = nb.Dot(m_kernel.Positions[vb[0]]);
            var planeB = new Plane3d(nb, db);
            var f = Eps.GenerationFactor;
            for (var i = 0; i < 3; i++)
            {
                f = f.Max(HeightFactor(planeB, m_kernel.Positions[va[i]]));
                f = f.Max(HeightFactor(planeA, m_kernel.Positions[vb[i]]));
            }
            return f.Min(Eps.MaxFactor);
        }

        private V3d WindingNormal(int _, Span<int> v)
        {
            var p0 = m_kernel.Positions[v[0]];
            return (m_kernel.Positions[v[1]] - p0).Cross(m_kernel.Positions[v[2]] - p0);
        }

        private double HeightFactor(in Plane3d plane, in V3d p)
        {
            var h = (plane.Normal.Dot(p) - plane.Distance).Abs();
            var unit = m_eps.Relative * (p.X.Abs() + p.Y.Abs() + p.Z.Abs() + plane.Distance.Abs() + m_eps.Scene);
            return unit > 0 ? h / unit : 0.0;
        }

        /// <summary>
        /// Pure (read-only, deterministic) part of a pair: signs, coplanarity,
        /// interval overlap. HeightSign is a pure function, so recomputing
        /// per pair is exactly as consistent as the former shared cache.
        /// </summary>
        private static readonly string? s_debugPoint = Environment.GetEnvironmentVariable("CSG_DEBUG_POINT");

        private bool NearDebugPoint(Span<int> vs)
        {
            if (s_debugPoint == null) return false;
            var parts = s_debugPoint.Split(',');
            var p = new V3d(double.Parse(parts[0]), double.Parse(parts[1]), double.Parse(parts[2]));
            var box = Box3d.Invalid;
            foreach (var v in vs) box.ExtendBy(m_kernel.Positions[v]);
            return box.EnlargedBy(1e-4).Contains(p);
        }

        private PairResult ComputePair(int ta, int tb)
        {
            var pa = m_kernel.TriPlane[ta];
            var pb = m_kernel.TriPlane[tb];

            Span<int> va = stackalloc int[] { m_kernel.T0[ta], m_kernel.T1[ta], m_kernel.T2[ta] };
            Span<int> vb = stackalloc int[] { m_kernel.T0[tb], m_kernel.T1[tb], m_kernel.T2[tb] };
            var dbgPt = NearDebugPoint(va) && NearDebugPoint(vb);
            if (dbgPt) Console.WriteLine($"NEARPAIR {ta},{tb} groups {m_planeGroup[pa]},{m_planeGroup[pb]}");

            // welded planes: the pair is coplanar by canonicalization —
            // orientation from the geometric windings (a welded face's plane
            // may be snapped against its winding)
            if (s_debugPair != null && s_debugPair == $"{ta},{tb}")
                Console.WriteLine($"PAIR {ta},{tb}: groups {m_planeGroup[pa]},{m_planeGroup[pb]}" +
                    (m_planeGroup[pa] == m_planeGroup[pb] ? $" overlap {CoplanarInteriorsOverlap(va, vb, pa)}" : ""));
            if (m_planeGroup[pa] == m_planeGroup[pb])
            {
                if (!CoplanarInteriorsOverlap(va, vb, pa)) return default;
                return new PairResult(WindingNormal(ta).Dot(WindingNormal(tb)) > 0, CoplanarSlabFactor(va, vb, pa));
            }

            Span<Sign3> sb = stackalloc Sign3[3];
            Span<Sign3> sa = stackalloc Sign3[3];
            for (var i = 0; i < 3; i++) sb[i] = Sign(pa, vb[i]);
            if (AllStrict(sb, Sign3.Above) || AllStrict(sb, Sign3.Below)) { if (dbgPt) Console.WriteLine($"  reject sb {sb[0]},{sb[1]},{sb[2]}"); return default; }
            for (var i = 0; i < 3; i++) sa[i] = Sign(pb, va[i]);
            if (AllStrict(sa, Sign3.Above) || AllStrict(sa, Sign3.Below)) { if (dbgPt) Console.WriteLine($"  reject sa {sa[0]},{sa[1]},{sa[2]}"); return default; }

            if (AllOn(sa) && AllOn(sb))
            {
                if (!CoplanarInteriorsOverlap(va, vb, pa)) return default;
                return new PairResult(WindingNormal(ta).Dot(WindingNormal(tb)) > 0, Eps.GenerationFactor);
            }

            var crossA = CrossingPoints(va, sa, pb);
            var crossB = CrossingPoints(vb, sb, pa);
            if (crossA.Count < 2 || crossB.Count < 2) { if (dbgPt) Console.WriteLine($"  reject cross {crossA.Count},{crossB.Count} sa {sa[0]},{sa[1]},{sa[2]} sb {sb[0]},{sb[1]},{sb[2]}"); return default; }


            var dir = m_kernel.Planes[pa].Normal.Cross(m_kernel.Planes[pb].Normal);
            var (loA, hiA) = Interval(crossA, dir);
            var (loB, hiB) = Interval(crossB, dir);
            var (lo, loCutPlane) = loA.T > loB.T ? (loA, pb) : (loB, pa);
            var (hi, hiCutPlane) = hiA.T < hiB.T ? (hiA, pb) : (hiB, pa);
            if (lo.T >= hi.T) { if (dbgPt) Console.WriteLine($"  reject interval {lo.T}..{hi.T}"); return default; }
            if (m_eps.AreCoincident(lo.P.Pos, hi.P.Pos, lo.P.Factor.Max(hi.P.Factor))) { if (dbgPt) Console.WriteLine($"  reject touch {lo.P.Pos} {hi.P.Pos}"); return default; }
            if (dbgPt) Console.WriteLine($"  segment {lo.P.Pos} -> {hi.P.Pos}");
            return new PairResult(lo.P, loCutPlane, hi.P, hiCutPlane);
        }

        /// <summary>Order-dependent part: vertex ids, welding, registration.</summary>
        private void MaterializePair(int ta, int tb, in PairResult r)
        {
            switch (r.Kind)
            {
                case PairKind.None: return;
                case PairKind.Coplanar:
                    m_coplanar.GetOrCreate(ta, _ => new List<(int, bool, double)>()).Add((tb, r.CoplanarSame, r.CoplanarFactor));
                    m_coplanar.GetOrCreate(tb, _ => new List<(int, bool, double)>()).Add((ta, r.CoplanarSame, r.CoplanarFactor));
                    // exactly-coplanar faces (slab within the baseline factor)
                    // keep the original path: side-face pairs provide the
                    // curves. Approximately welded faces need explicit outline
                    // constraints so coverage boundaries weld consistently
                    // with the neighboring real cuts.
                    if (s_debugReg && (ta == 77 || tb == 77))
                        Console.WriteLine($"COPL ta {ta} tb {tb} same={r.CoplanarSame} slab={r.CoplanarFactor:0.#}");
                    if (r.CoplanarFactor > 4 * Eps.GenerationFactor)
                    {
                        AddCoplanarOutline(ta, tb, r.CoplanarFactor);
                        AddCoplanarOutline(tb, ta, r.CoplanarFactor);
                        Bump(m_faceFactorOverride, ta, r.CoplanarFactor);
                        Bump(m_faceFactorOverride, tb, r.CoplanarFactor);
                    }
                    return;
                case PairKind.Segment:
                    var v0 = Materialize(r.Lo, r.LoCutPlane);
                    var v1 = Materialize(r.Hi, r.HiCutPlane);
                    var dbgSeg = s_debugPoint != null && NearDebugPoint(stackalloc[] { v0, v1 });
                    if (dbgSeg)
                        Console.WriteLine($"MATSEG {ta},{tb}: v0 {v0}@{m_kernel.Positions[v0]} v1 {v1}@{m_kernel.Positions[v1]} " +
                            $"wf {WithinFace(ta, v0)},{WithinFace(ta, v1)},{WithinFace(tb, v0)},{WithinFace(tb, v1)}");
                    if (v0 == v1) return;
                    // endpoints marginally outside a face (an exit point on
                    // the partner's edge can scatter past it by more than its
                    // creation factor) get honestly widened factors instead of
                    // killing a real segment — that would puncture the cut
                    // curve and break region classification. Only endpoints
                    // beyond MaxFactor absorption drop the segment.
                    bool InFace(int tri2, int vid2) => WithinFace(tri2, vid2) || EnsureInsertable(tri2, vid2);
                    if (!InFace(ta, v0) || !InFace(ta, v1)
                        || !InFace(tb, v0) || !InFace(tb, v1)) return;
                    AddConstraint(ta, v0, v1);
                    AddConstraint(tb, v0, v1);
                    return;
                default: throw new InvalidOperationException();
            }
        }

        private static bool AllStrict(Span<Sign3> s, Sign3 v) => s[0] == v && s[1] == v && s[2] == v;
        private static bool AllOn(Span<Sign3> s) => s[0] == Sign3.On && s[1] == Sign3.On && s[2] == Sign3.On;

        /// <summary>
        /// Clips each edge of tb to triangle ta in the shared plane and adds
        /// the clipped segments as constraints on ta. Clip points are created
        /// with the tolerance-factor cap (they live on welded planes) and are
        /// registered on tb's edges so both sides subdivide consistently.
        /// </summary>
        private readonly Dictionary<int, double> m_faceFactorOverride = new();

        private static void Bump(Dictionary<int, double> d, int key, double f)
        {
            d.TryGetValue(key, out var cur);
            if (f > cur) d[key] = f;
        }

        private void AddCoplanarOutline(int ta, int tb, double factor)
        {
            var normal = m_kernel.Planes[m_kernel.TriPlane[ta]].Normal;
            Span<int> va = stackalloc int[] { m_kernel.T0[ta], m_kernel.T1[ta], m_kernel.T2[ta] };
            Span<V2d> a2 = stackalloc V2d[3];
            for (var i = 0; i < 3; i++) a2[i] = Triangulator.ProjectDominant(normal, m_kernel.Positions[va[i]]);
            // orient CCW for inside-is-left clipping
            var det = (a2[1] - a2[0]).X * (a2[2] - a2[0]).Y - (a2[1] - a2[0]).Y * (a2[2] - a2[0]).X;
            if (det == 0) return;
            if (det < 0) { (a2[1], a2[2]) = (a2[2], a2[1]); (va[1], va[2]) = (va[2], va[1]); }

            Span<int> vb = stackalloc int[] { m_kernel.T0[tb], m_kernel.T1[tb], m_kernel.T2[tb] };
            for (var e = 0; e < 3; e++)
            {
                var u = vb[e]; var v = vb[(e + 1) % 3];
                var pu = Triangulator.ProjectDominant(normal, m_kernel.Positions[u]);
                var pv = Triangulator.ProjectDominant(normal, m_kernel.Positions[v]);
                var t0 = 0.0; var t1 = 1.0;
                for (var i = 0; i < 3 && t0 < t1; i++)
                {
                    var p = a2[i]; var q = a2[(i + 1) % 3];
                    var d = q - p;
                    var du = d.X * (pu.Y - p.Y) - d.Y * (pu.X - p.X);
                    var dv = d.X * (pv.Y - p.Y) - d.Y * (pv.X - p.X);
                    if (du < 0 && dv < 0) { t0 = 1; t1 = 0; break; }
                    if (du < 0) t0 = t0.Max(du / (du - dv));
                    else if (dv < 0) t1 = t1.Min(du / (du - dv));
                }
                if (t1 - t0 <= 1e-9) continue;

                var w0 = OutlinePoint(u, v, t0, factor);
                var w1 = OutlinePoint(u, v, t1, factor);
                if (w0 != w1 && EnsureInsertable(ta, w0) && EnsureInsertable(ta, w1))
                    AddConstraint(ta, w0, w1);
            }
        }

        private int OutlinePoint(int u, int v, double t, double factor)
        {
            // endpoints participate in welded-plane geometry: their position
            // relative to that neighborhood is only known to the slab factor
            if (t <= 0) { BumpFactor(u, (4 * factor).Min(Eps.MaxFactor)); return u; }
            if (t >= 1) { BumpFactor(v, (4 * factor).Min(Eps.MaxFactor)); return v; }
            var p = m_kernel.Positions[u] + t * (m_kernel.Positions[v] - m_kernel.Positions[u]);
            if (m_eps.AreCoincident(p, m_kernel.Positions[u], factor)) return u;
            if (m_eps.AreCoincident(p, m_kernel.Positions[v], factor)) return v;
            var vid = GridFindCoincident(p, factor);
            if (vid < 0)
            {
                vid = m_kernel.Positions.Count;
                m_kernel.Positions.Add(p);
                m_kernel.TolFactor.Add(factor);
                GridAdd(vid);
            }
            RegisterEdgePoint(u, v, vid);
            return vid;
        }

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

        private void BumpFactor(int vid, double factor)
        {
            if (m_kernel.TolFactor[vid] < factor) m_kernel.TolFactor[vid] = factor;
        }

        private void RegisterEdgePoint(int a, int b, int vid)
            => m_edgePoints.GetOrCreate(SortedEdge(a, b), _ => new HashSet<int>()).Add(vid);

        private static (int, int) SortedEdge(int a, int b) => a < b ? (a, b) : (b, a);

        internal static long EdgeKey(int a, int b)
            => a < b ? ((long)a << 32) | (uint)b : ((long)b << 32) | (uint)a;

        private void AddConstraint(int tri, int v0, int v1)
        {
            if (s_debugFace == tri.ToString())
                Console.WriteLine($"CON tri {tri}: ({v0},{v1})");
            m_faceConstraints.GetOrCreate(tri, _ => new HashSet<(int, int)>()).Add(SortedEdge(v0, v1));
        }

        private static readonly bool s_debugReg = Environment.GetEnvironmentVariable("CSG_DEBUG_REG") != null;
        private static readonly string? s_debugFrag = Environment.GetEnvironmentVariable("CSG_DEBUG_FRAG");
        private static readonly string? s_debugPair = Environment.GetEnvironmentVariable("CSG_DEBUG_PAIR");

        #endregion

        #region subdivision

        private void Subdivide()
        {
            LateWeld();
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
                            // the edge's working resolution counts too: a cut
                            // point hugging an edge whose endpoints carry wide
                            // factors forms a sub-resolution lens unless it is
                            // registered (and thus flattened) onto the edge
                            var tjf = Fun.Max(m_kernel.TolFactor[e], Eps.GenerationFactor)
                                .Max(m_kernel.TolFactor[v[i]]).Max(m_kernel.TolFactor[v[j]]);
                            // 2D-only tests are blind along the projection
                            // axis: the point must also lie on the face plane
                            if (m_eps.HeightSign(plane, m_kernel.Positions[e], tjf) != Sign3.On) continue;
                            if (m_eps.AreaSign(p[i], p[j], q, tjf) != Sign3.On) continue;
                            var d = p[j] - p[i]; var w = q - p[i];
                            var dot = d.Dot(w);
                            if (dot <= 0 || dot >= d.LengthSquared)
                            {
                                // in the edge band but beyond an endpoint: the
                                // point claims that corner's location, so its
                                // distance to it measures real scatter — widen
                                // the factor so the late weld can unify them
                                var corner = dot <= 0 ? v[i] : v[j];
                                var cdist = (m_kernel.Positions[e] - m_kernel.Positions[corner]).NormMax;
                                var needed = Eps.GenerationFactor * cdist
                                    / (m_eps.Relative * (2 * m_kernel.Positions[e].NormMax + m_eps.Scene));
                                // cap at MaxFactor: the weld's own coincidence
                                // test decides whether the capped band reaches;
                                // beyond GenerationFactor*MaxFactor the point
                                // is genuinely distinct — no bump
                                if (needed <= Eps.GenerationFactor * Eps.MaxFactor && needed > m_kernel.TolFactor[e])
                                    BumpFactor(e, needed.Min(Eps.MaxFactor));
                                continue;
                            }
                            if (s_debugFace == tri.ToString())
                                Console.WriteLine($"TJREG tri {tri}: {e} on edge ({v[i]},{v[j]})");
                            RegisterEdgePoint(v[i], v[j], e);
                        }
                    }
                }
            }

            if (Environment.GetEnvironmentVariable("CSG_DEBUG_REGIONS") != null)
                Console.WriteLine($"FACECON-PRE faces {m_faceConstraints.Count} segs {System.Linq.Enumerable.Sum(m_faceConstraints.Values, v => v.Count)}");
            DedupeEdgeAssignments();
            // the dedupe pass measured edge scatter and widened factors —
            // re-weld so points that became coincident under honest factors
            // unify before any face consumes them
            LateWeld();
            InsertabilityPrepass();
            WeldEdgeResolution();
            // welding redirects points to representatives the first pass never
            // saw for a given face — re-check on the final ids (idempotent)
            InsertabilityPrepass();
            // the prepasses measured and widened factors again: points whose
            // FINAL tolerances overlap must unify before faces consume them.
            // welding widens representative factors, which can make further
            // pairs overlap — iterate to a fixpoint (bounded: factors are
            // capped at MaxFactor, aliases only grow)
            for (var round = 0; round < 4; round++)
            {
                var seen = m_lateAlias.Count;
                LateWeld();
                FaceFeasibilityWeld();
                if (m_lateAlias.Count == seen) break;
            }
            // weld remaps can merge a welded point's registrations onto its
            // representative, recreating multi-edge conflicts — dedupe again
            DedupeEdgeAssignments();

            // per-face triangulations run in parallel (read-only kernel,
            // private CDT state); assembly stays in face order → deterministic
            var results = new (List<(int, int, int)> Tris, List<(int, int)> Constraints)?[m_kernel.TriangleCount];
            Lap2("subdiv-prep");
            CsgParallel.For(0, m_kernel.TriangleCount, m_maxThreads, tri =>
            {
                if (m_deadTri.Contains(tri)) return;
                var constraints = m_faceConstraints.GetOrDefault(tri);
                var boundary = BoundaryPoints(tri);
                if (constraints == null && boundary == null) return;

                m_diagTri = tri;
                var plane = m_kernel.Planes[m_kernel.TriPlane[tri]];
                V2d Proj(int vid) => Triangulator.ProjectDominant(plane.Normal, m_kernel.Positions[vid]);

                var faceFactor = FaceFactor(tri);

                if (s_debugFace == tri.ToString())
                {
                    Console.WriteLine($"FACE {tri}: factor {faceFactor:0.#}");
                    if (constraints != null)
                        foreach (var (ca, cb) in constraints)
                            Console.WriteLine($"  constraint ({ca},{cb}) {m_kernel.Positions[ca]} f{m_kernel.TolFactor[ca]:0.#} - {m_kernel.Positions[cb]} f{m_kernel.TolFactor[cb]:0.#}");
                    if (boundary != null)
                        foreach (var vid in boundary)
                            Console.WriteLine($"  boundary {vid} {m_kernel.Positions[vid]} f{m_kernel.TolFactor[vid]:0.#}");
                    Span<int> dv = stackalloc int[] { m_kernel.T0[tri], m_kernel.T1[tri], m_kernel.T2[tri] };
                    for (var i = 0; i < 3; i++)
                    {
                        var pts = m_edgePoints.GetOrDefault(SortedEdge(dv[i], dv[(i + 1) % 3]));
                        if (pts != null)
                            Console.WriteLine($"  edge ({dv[i]},{dv[(i + 1) % 3]}): {string.Join(",", pts)}");
                    }
                }
                var cdt = FaceCdt.Rent(m_eps, faceFactor,
                    m_kernel.T0[tri], Proj(m_kernel.T0[tri]),
                    m_kernel.T1[tri], Proj(m_kernel.T1[tri]),
                    m_kernel.T2[tri], Proj(m_kernel.T2[tri]));

                // boundary points are committed to their edge: subdivide each
                // face edge as an explicit chain ordered by 3D parameter —
                // identical order and positions in both faces sharing the
                // edge, with 2D positions lerped exactly onto the edge
                Span<int> cv = stackalloc int[] { m_kernel.T0[tri], m_kernel.T1[tri], m_kernel.T2[tri] };
                for (var i = 0; i < 3; i++)
                {
                    var eu = cv[i]; var ev = cv[(i + 1) % 3];
                    var pts = m_edgePoints.GetOrDefault(SortedEdge(eu, ev));
                    if (pts == null || pts.Count == 0) continue;
                    var (su, sv) = SortedEdge(eu, ev);
                    var pu3 = m_kernel.Positions[su];
                    var dir3 = m_kernel.Positions[sv] - pu3;
                    var len23 = dir3.LengthSquared;
                    var ordered = new List<(double T, int Vid)>(pts.Count);
                    foreach (var vid in pts)
                        ordered.Add(((dir3.Dot(m_kernel.Positions[vid] - pu3) / len23).Clamp(0.0, 1.0), vid));
                    ordered.Sort();
                    var u2 = Proj(su); var v2 = Proj(sv);
                    // identity was decided globally; the chain only places
                    // the surviving points, with t clamped off the exact
                    // corners so no per-face corner aliasing can occur —
                    // decisions on the shared t are identical in both faces
                    var prev = su;
                    foreach (var (t0, vid) in ordered)
                    {
                        if (cdt.KnowsKernel(vid)) continue;
                        var t = t0.Clamp(1e-9, 1 - 1e-9);
                        try { cdt.InsertOnEdge(prev, sv, vid, u2 + t * (v2 - u2), 1 << i); }
                        catch (CsgVerificationException e) { throw new CsgVerificationException(Diag(e, vid)); }
                        prev = vid;
                    }
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
                    if (s_debugFace == tri.ToString())
                        foreach (var (a, b, c) in results[tri]!.Value.Tris)
                            Console.WriteLine($"  outtri {a},{b},{c}");
                }
                catch (CsgVerificationException e)
                {
                    throw new CsgVerificationException(Diag(e));
                }
            });

            Lap2("subdiv-cdt");
            var counts = new int[m_kernel.TriangleCount + 1];
            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
                counts[tri + 1] = counts[tri] + (results[tri] != null ? results[tri]!.Value.Tris.Count
                    : m_deadTri.Contains(tri) ? 0 : 1);
            var fragmentArray = new Fragment[counts[m_kernel.TriangleCount]];
            CsgParallel.For(0, m_kernel.TriangleCount, m_maxThreads, tri =>
            {
                var at = counts[tri];
                if (results[tri] == null)
                {
                    if (m_deadTri.Contains(tri)) return;
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
            if (Environment.GetEnvironmentVariable("CSG_DEBUG_REGIONS") != null)
            {
                for (var m2 = 0; m2 < m_barriers.Length; m2++)
                    Console.WriteLine($"BARRIERS mesh {m2}: {m_barriers[m2].Count}");
                Console.WriteLine($"FACECON faces {m_faceConstraints.Count} segs {System.Linq.Enumerable.Sum(m_faceConstraints.Values, v => v.Count)}");
                var fragEdges = new HashSet<long>[m_barriers.Length];
                for (var m2 = 0; m2 < m_barriers.Length; m2++) fragEdges[m2] = new HashSet<long>();
                foreach (var fr in Fragments)
                {
                    var fm = m_kernel.TriMesh[fr.Parent];
                    fragEdges[fm].Add(EdgeKey(fr.V0, fr.V1));
                    fragEdges[fm].Add(EdgeKey(fr.V1, fr.V2));
                    fragEdges[fm].Add(EdgeKey(fr.V2, fr.V0));
                }
                for (var m2 = 0; m2 < m_barriers.Length; m2++)
                {
                    var deg = new Dictionary<int, int>();
                    foreach (var k in m_barriers[m2])
                    {
                        var u = (int)(k >> 32); var w = (int)k;
                        deg[u] = (deg.TryGetValue(u, out var du) ? du : 0) + 1;
                        deg[w] = (deg.TryGetValue(w, out var dw) ? dw : 0) + 1;
                    }
                    var odd = new List<int>();
                    foreach (var (v2, d2) in deg) if ((d2 & 1) != 0) odd.Add(v2);
                    Console.WriteLine($"BARRIERDEG mesh {m2}: {odd.Count} odd-degree of {deg.Count}");
                    for (var oi = 0; oi < odd.Count && oi < 6; oi++)
                    {
                        var ov = odd[oi];
                        Console.WriteLine($"  odd {ov}@{m_kernel.Positions[ov]} f{m_kernel.TolFactor[ov]:0.#} rep {RepLate(ov)}");
                        foreach (var (tri2, segs2) in m_faceConstraints)
                            foreach (var (ca2, cb2) in segs2)
                                if (ca2 == ov || cb2 == ov)
                                    Console.WriteLine($"    con tri {tri2} mesh {m_kernel.TriMesh[tri2]}: ({ca2},{cb2})");
                    }
                }
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

                // (1) proper crossings within this face (outline constraints
                // can cross ordinary segments even with two solids)
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
                        if (m_eps.HeightSign(m_kernel.Planes[m_kernel.TriPlane[tri]], m_kernel.Positions[v], pf) != Sign3.On) continue;
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

        /// <summary>
        /// Face-level adaptive tolerance: the widest factor among the points
        /// this face must integrate (grazing cuts widen it).
        /// </summary>
        private double FaceFactor(int tri)
        {
            var f = Eps.GenerationFactor;
            var constraints = m_faceConstraints.GetOrDefault(tri);
            if (constraints != null)
                foreach (var (ca, cb) in constraints)
                    f = f.Max(m_kernel.TolFactor[ca]).Max(m_kernel.TolFactor[cb]);
            var boundary = BoundaryPoints(tri);
            if (boundary != null)
                foreach (var vid in boundary) f = f.Max(m_kernel.TolFactor[vid]);
            if (m_faceFactorOverride.TryGetValue(tri, out var over)) f = f.Max(over);
            return f;
        }

        /// <summary>
        /// Late global weld: derived points are created and grid-welded at
        /// creation-time factors, but later passes measure and widen those
        /// factors. Two points whose FINAL tolerances overlap are semantically
        /// one point — leaving them distinct lets different faces pick
        /// different ids for the same geometric location, which surfaces as
        /// cracks (open edges) along shared curves. Input vertices are never
        /// aliased away; the smallest id wins.
        /// </summary>
        private readonly Dictionary<int, int> m_lateAlias = new();

        private int RepLate(int v) { while (m_lateAlias.TryGetValue(v, out var r)) v = r; return v; }

        private void LateWeld()
        {
            var inputCount = 0;
            foreach (var c in m_kernel.VertexCount) inputCount += c;
            var alias = m_lateAlias;
            var before = alias.Count;
            int Rep(int v) { while (alias.TryGetValue(v, out var r)) v = r; return v; }

            for (var vid = 0; vid < m_kernel.Positions.Count; vid++)
            {
                var f = m_kernel.TolFactor[vid];
                // creation-time welding already covered baseline factors
                if (f <= Eps.GenerationFactor || alias.ContainsKey(vid)) continue;
                var p = m_kernel.Positions[vid];
                var tol = m_eps.Relative * (p.NormMax + 2 * m_eps.Scene) * f;
                var fine = f <= 3 * Eps.GenerationFactor;
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
                            {
                                if (j == vid) continue;
                                var pf = f.Max(m_kernel.TolFactor[j]);
                                if (!m_eps.AreCoincident(p, m_kernel.Positions[j], pf)) continue;
                                var lo = Math.Min(vid, j);
                                var hi = Math.Max(vid, j);
                                if (alias.ContainsKey(hi)) continue;
                                var target = Rep(lo);
                                if (target == hi) continue;
                                alias[hi] = target;
                                m_kernel.TolFactor[target] = m_kernel.TolFactor[target].Max(m_kernel.TolFactor[hi]);
                                if (hi < inputCount) m_inputWelded = true;
                            }
                        }
            }
            if (alias.Count == before) return;
            ApplyLateAlias();
            if (m_inputWelded) RemapKernelTopology();
        }

        /// <summary>Rewrites constraint segments and edge registries through the global alias map.</summary>
        private void ApplyLateAlias()
        {
            foreach (var (_, constraints) in m_faceConstraints)
            {
                var segs = new List<(int, int)>(constraints);
                constraints.Clear();
                foreach (var (a, b) in segs)
                {
                    var ra = RepLate(a);
                    var rb = RepLate(b);
                    if (ra != rb) constraints.Add(SortedEdge(ra, rb));
                }
            }
            foreach (var (edge, pts) in m_edgePoints)
            {
                var (u, w) = edge;
                var mapped = new List<int>();
                foreach (var p in pts)
                {
                    var r = RepLate(p);
                    if (r != u && r != w && !mapped.Contains(r)) mapped.Add(r);
                }
                pts.Clear();
                foreach (var p in mapped) pts.Add(p);
            }
        }

        /// <summary>
        /// Identity decisions are global, faces only triangulate: two points a
        /// face consumes that lie within the face's area-predicate feasibility
        /// radius cannot be separated by any predicate there — merging them
        /// per-face would let different faces disagree about identity (cracks),
        /// so they are welded in kernel space for everyone. Input vertices are
        /// never welded away.
        /// </summary>
        private void FaceFeasibilityWeld()
        {
            var inputCount = 0;
            foreach (var c in m_kernel.VertexCount) inputCount += c;
            var before = m_lateAlias.Count;
            var pts = new List<int>();
            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
            {
                var constraints = m_faceConstraints.GetOrDefault(tri);
                var boundary = BoundaryPoints(tri);
                if (constraints == null && boundary == null) continue;

                pts.Clear();
                pts.Add(m_kernel.T0[tri]); pts.Add(m_kernel.T1[tri]); pts.Add(m_kernel.T2[tri]);
                if (boundary != null) foreach (var v in boundary) if (!pts.Contains(v)) pts.Add(v);
                if (constraints != null)
                    foreach (var (ca, cb) in constraints)
                    {
                        if (!pts.Contains(ca)) pts.Add(ca);
                        if (!pts.Contains(cb)) pts.Add(cb);
                    }

                var p0 = m_kernel.Positions[m_kernel.T0[tri]];
                var p1 = m_kernel.Positions[m_kernel.T1[tri]];
                var p2 = m_kernel.Positions[m_kernel.T2[tri]];
                var mag = Fun.Max(p0.NormMax, p1.NormMax, p2.NormMax);
                var ext = Fun.Max((p1 - p0).NormMax, (p2 - p0).NormMax, (p2 - p1).NormMax);
                var radius = 4 * m_eps.Relative * (mag + ext + m_eps.Scene) * FaceFactor(tri);
                if (s_debugFace == tri.ToString())
                {
                    Console.WriteLine($"FFW {tri}: radius {radius:E2} factor {FaceFactor(tri):0.#} pts {string.Join(",", pts)}");
                    for (var i = 0; i < pts.Count; i++)
                        Console.WriteLine($"  ffw pt {pts[i]} {m_kernel.Positions[RepLate(pts[i])]} f{m_kernel.TolFactor[RepLate(pts[i])]:0.#}");
                }

                for (var i = 0; i < pts.Count; i++)
                    for (var j = i + 1; j < pts.Count; j++)
                    {
                        var a = RepLate(pts[i]); var b = RepLate(pts[j]);
                        if (a == b) continue;
                        var lo = Math.Min(a, b); var hi = Math.Max(a, b);
                        if (m_lateAlias.ContainsKey(hi)) continue;
                        if ((m_kernel.Positions[lo] - m_kernel.Positions[hi]).NormMax > radius) continue;
                        m_lateAlias[hi] = lo;
                        m_kernel.TolFactor[lo] = m_kernel.TolFactor[lo].Max(m_kernel.TolFactor[hi]);
                        if (hi < inputCount) m_inputWelded = true;
                    }
            }
            if (m_lateAlias.Count != before) ApplyLateAlias();
            if (m_inputWelded) RemapKernelTopology();
        }

        private bool m_inputWelded;
        private readonly HashSet<int> m_deadTri = new();

        /// <summary>
        /// Input-vertex welds are edge collapses: rewrite triangle corners
        /// through the alias map, retire triangles that lost a dimension, and
        /// remap edge-registry keys. Collapsing an edge whose endpoints'
        /// widened tolerances overlap moves geometry strictly within the
        /// working resolution.
        /// </summary>
        private void RemapKernelTopology()
        {
            m_inputWelded = false;
            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
            {
                var t0 = RepLate(m_kernel.T0[tri]);
                var t1 = RepLate(m_kernel.T1[tri]);
                var t2 = RepLate(m_kernel.T2[tri]);
                m_kernel.T0[tri] = t0; m_kernel.T1[tri] = t1; m_kernel.T2[tri] = t2;
                if (t0 == t1 || t1 == t2 || t2 == t0)
                {
                    m_deadTri.Add(tri);
                    m_faceConstraints.Remove(tri);
                }
            }
            var oldEdges = new List<((int, int) Edge, HashSet<int> Pts)>();
            foreach (var (edge, pts) in m_edgePoints) oldEdges.Add((edge, pts));
            m_edgePoints.Clear();
            foreach (var (edge, pts) in oldEdges)
            {
                var u = RepLate(edge.Item1);
                var w = RepLate(edge.Item2);
                if (u == w) continue;
                var target = m_edgePoints.GetOrCreate(SortedEdge(u, w), _ => new HashSet<int>());
                foreach (var p in pts)
                {
                    var r = RepLate(p);
                    if (r != u && r != w) target.Add(r);
                }
            }
        }

        /// <summary>
        /// Insertability prepass (sequential — bumps shared factors): every
        /// point a face will consume must be absorbable by that face; clip
        /// scatter and cross-projection drift can push points marginally
        /// outside, which EnsureInsertable converts into a measured factor.
        /// </summary>
        /// <summary>
        /// A point registered on several edges (corner-region T-junction
        /// registrations) must subdivide exactly one of them, chosen globally
        /// (3D distance to the edge segment — face-independent), or the faces
        /// sharing those edges disagree about their subdivision chains.
        /// </summary>
        private void DedupeEdgeAssignments()
        {
            // a point may legitimately subdivide several edges at once
            // (edge-face intersections across meshes, edge-edge cuts); only
            // registrations on edges SHARING a vertex are corner-region
            // conflicts where faces would disagree about their chains
            var edgesOf = new Dictionary<int, List<(int, int)>>();
            foreach (var (edge, pts) in m_edgePoints)
                foreach (var vid in pts)
                    edgesOf.GetOrCreate(vid, _ => new List<(int, int)>()).Add(edge);

            double Dist2(int vid, (int, int) edge)
            {
                var a = m_kernel.Positions[edge.Item1];
                var d = m_kernel.Positions[edge.Item2] - a;
                var w = m_kernel.Positions[vid] - a;
                var t = (d.Dot(w) / d.LengthSquared.Max(1e-300)).Clamp(0.0, 1.0);
                return (w - t * d).LengthSquared;
            }

            // registration is an On-commitment to the edge: the measured
            // distance to it is a lower bound on the point's real scatter, so
            // widen dishonest factors here — the resolution weld then absorbs
            // corner-region points globally instead of per-face guesswork
            foreach (var (edge, pts) in m_edgePoints)
                foreach (var vid in pts)
                {
                    var dist = Dist2(vid, edge).Sqrt();
                    var p = m_kernel.Positions[vid];
                    var needed = Eps.GenerationFactor * dist
                        / (m_eps.Relative * (2 * p.NormMax + m_eps.Scene));
                    if (needed > m_kernel.TolFactor[vid])
                        BumpFactor(vid, needed.Min(Eps.MaxFactor));
                }

            foreach (var (vid, edges) in edgesOf)
            {
                if (edges.Count < 2) continue;
                edges.Sort();
                for (var i = 0; i < edges.Count; i++)
                    for (var j = i + 1; j < edges.Count; j++)
                    {
                        var (a, b) = edges[i]; var (c, d) = edges[j];
                        if (a != c && a != d && b != c && b != d) continue;
                        var loser = Dist2(vid, edges[i]) <= Dist2(vid, edges[j]) ? edges[j] : edges[i];
                        if (s_debugReg) Console.WriteLine($"DEDUPE {vid}: ({a},{b}) vs ({c},{d}) -> drop ({loser.Item1},{loser.Item2})");
                        m_edgePoints[loser].Remove(vid);
                    }
            }
        }

        private void InsertabilityPrepass()
        {
            // boundary points need no check: they are chain-inserted exactly
            // on their committed edge, which is always inside the face —
            // constraint endpoints that are edge-registered count as boundary
            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
            {
                var constraints = m_faceConstraints.GetOrDefault(tri);
                if (constraints == null) continue;
                var onEdge = BoundaryPoints(tri);
                var plane = m_kernel.Planes[m_kernel.TriPlane[tri]];
                bool Ok(int vid) => vid == m_kernel.T0[tri] || vid == m_kernel.T1[tri] || vid == m_kernel.T2[tri]
                    || ((onEdge != null && onEdge.Contains(vid))
                        || m_eps.HeightSign(plane, m_kernel.Positions[vid], m_kernel.TolFactor[vid].Max(Eps.GenerationFactor)) == Sign3.On)
                       && EnsureInsertable(tri, vid);
                constraints.RemoveWhere(seg => !Ok(seg.Item1) || !Ok(seg.Item2));
            }
        }

        /// <summary>
        /// Global resolution welding along subdivided input edges: a split
        /// point within the working resolution of an edge endpoint (at the
        /// widest factor of any face consuming the edge) cannot form a
        /// recoverable sub-edge in the face CDTs — its flap triangle is
        /// degenerate at that factor and constraint recovery on the
        /// sub-resolution segment has no strict crossing to flip. Such points
        /// are aliased to the endpoint (or to their neighbor split point)
        /// globally, so every face sharing the edge subdivides identically.
        /// </summary>
        private void WeldEdgeResolution()
        {
            if (m_edgePoints.Count == 0) return;

            // widest face factor consuming each subdivided edge
            var edgeFactor = new Dictionary<(int, int), double>();
            for (var tri = 0; tri < m_kernel.TriangleCount; tri++)
            {
                Span<int> v = stackalloc int[] { m_kernel.T0[tri], m_kernel.T1[tri], m_kernel.T2[tri] };
                var touches = false;
                for (var i = 0; i < 3 && !touches; i++)
                    touches = m_edgePoints.ContainsKey(SortedEdge(v[i], v[(i + 1) % 3]));
                if (!touches) continue;
                var f = FaceFactor(tri);
                for (var i = 0; i < 3; i++)
                {
                    var e = SortedEdge(v[i], v[(i + 1) % 3]);
                    if (!m_edgePoints.ContainsKey(e)) continue;
                    edgeFactor[e] = edgeFactor.TryGetValue(e, out var old) ? old.Max(f) : f;
                }
            }

            var alias = new Dictionary<int, int>();
            int Rep(int v) { while (alias.TryGetValue(v, out var r)) v = r; return v; }

            bool SubRes(int p, int q, double f)
            {
                var a = m_kernel.Positions[p];
                var b = m_kernel.Positions[q];
                var tol = m_eps.Relative * (a.NormMax + b.NormMax + m_eps.Scene) * f * Eps.GenerationFactor;
                return (a - b).NormMax <= tol;
            }

            foreach (var (edge, pts) in m_edgePoints)
            {
                if (!edgeFactor.TryGetValue(edge, out var f)) continue;
                var (u, w) = edge;
                var dir = m_kernel.Positions[w] - m_kernel.Positions[u];
                var sorted = new List<int>(pts);
                sorted.Sort((x, y) => dir.Dot(m_kernel.Positions[x]).CompareTo(dir.Dot(m_kernel.Positions[y])));
                var prev = u;
                foreach (var p0 in sorted)
                {
                    var p = Rep(p0);
                    if (p == u || p == w || alias.ContainsKey(p)) continue;
                    var target = SubRes(p, prev, f) ? prev : SubRes(p, w, f) ? w : -1;
                    if (target >= 0)
                    {
                        // resolve the target too: aliases must always point at
                        // a currently-unaliased representative or later
                        // assignments can close a cycle
                        target = Rep(target);
                        if (target == p) continue;
                        alias[p] = target;
                        m_kernel.TolFactor[target] = m_kernel.TolFactor[target].Max(m_kernel.TolFactor[p]);
                    }
                    else prev = p;
                }
            }
            if (alias.Count == 0) return;

            // rewrite the registries through the alias map
            foreach (var (edge, pts) in m_edgePoints)
            {
                var (u, w) = edge;
                var mapped = new List<int>();
                foreach (var p in pts)
                {
                    var r = Rep(p);
                    if (r != u && r != w && !mapped.Contains(r)) mapped.Add(r);
                }
                pts.Clear();
                foreach (var p in mapped) pts.Add(p);
            }
            foreach (var (_, constraints) in m_faceConstraints)
            {
                var segs = new List<(int, int)>(constraints);
                constraints.Clear();
                foreach (var (a, b) in segs)
                {
                    var ra = Rep(a);
                    var rb = Rep(b);
                    if (ra != rb) constraints.Add(SortedEdge(ra, rb));
                }
            }
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

        /// <summary>
        /// Self-resolution classification: each fragment is a piece of the
        /// (possibly self-intersecting) input surface. The resolved solid is
        /// the positive-winding region {w ≥ 1}; a fragment lies on its boundary
        /// iff its two sides straddle winding 1. The winding number at a point
        /// is the signed count of surface crossings of a ray to infinity
        /// (+1 inside a single outward-oriented shell, 0 outside).
        /// </summary>
        private void SelfClassify()
        {
            SelfKeep = new bool[Fragments.Count];
            SelfFlip = new bool[Fragments.Count];
            var diag = m_bvh[0].RootBox.Size.Length;
            CsgParallel.For(0, Fragments.Count, m_maxThreads, f =>
            {
                var fr = Fragments[f];
                var p0 = m_kernel.Positions[fr.V0]; var p1 = m_kernel.Positions[fr.V1]; var p2 = m_kernel.Positions[fr.V2];
                var c = (p0 + p1 + p2) / 3.0;
                var nrm = (p1 - p0).Cross(p2 - p0);
                var len = nrm.Length;
                if (len <= 0) return; // degenerate fragment
                nrm /= len;
                var edge = Fun.Min((p1 - p0).Length, (p2 - p1).Length, (p0 - p2).Length);
                // sample winding on both sides; the correct offset lands one in
                // each adjacent cell, so the windings must differ by exactly 1
                // (crossing one sheet). At crowded triple lines a too-large
                // offset crosses an extra sheet — shrink until the invariant
                // holds; a too-small offset stays in one cell (diff 0) — grow.
                var wMinus = 0; var wPlus = 0; var ok = false;
                var eps = (1e-2 * edge).Max(1e-9 * diag);
                for (var attempt = 0; attempt < 20 && eps > 1e-13 * diag; attempt++)
                {
                    var wm = WindingAt(c - eps * nrm);
                    var wp = WindingAt(c + eps * nrm);
                    if (wm == null || wp == null) { eps *= 0.5; continue; }
                    var d = wm.Value - wp.Value;
                    if (d == 1 || d == -1) { wMinus = wm.Value; wPlus = wp.Value; ok = true; break; }
                    eps *= d == 0 ? 4.0 : 0.5;
                }
                if (!ok) return; // could not isolate the two adjacent cells → drop
                var inMinus = wMinus >= 1;
                var inPlus = wPlus >= 1;
                if (inMinus == inPlus) return; // interior or exterior fragment → not on the boundary
                SelfKeep[f] = true;
                SelfFlip[f] = inPlus; // normal points into the solid → reverse it outward
            });
        }

        /// <summary>
        /// Winding number of p against the original surface: signed count of
        /// forward ray crossings, +1 per exit and −1 per entry. Returns null
        /// when every probe direction grazes the surface at p.
        /// </summary>
        private int? WindingAt(in V3d p)
        {
            foreach (var dir in s_rayDirs)
            {
                var w = 0; var grazed = false;
                foreach (var t in m_bvh[0].RayCandidates(p, dir, m_triOf[0]))
                {
                    var q0 = m_kernel.Positions[m_kernel.T0[t]];
                    var q1 = m_kernel.Positions[m_kernel.T1[t]];
                    var q2 = m_kernel.Positions[m_kernel.T2[t]];
                    var nrm = (q1 - q0).Cross(q2 - q0);
                    var denom = nrm.Dot(dir);
                    if (denom.Abs() < 1e-300) continue; // ray parallel to the face
                    var s = nrm.Dot(q0 - p) / denom;
                    if (s <= 1e-12) continue; // behind or through the origin
                    var hit = p + s * dir;
                    var a = Triangulator.ProjectDominant(nrm, q0);
                    var b = Triangulator.ProjectDominant(nrm, q1);
                    var cc = Triangulator.ProjectDominant(nrm, q2);
                    var h = Triangulator.ProjectDominant(nrm, hit);
                    var s0 = m_eps.AreaSign(a, b, h, Eps.GenerationFactor);
                    var s1 = m_eps.AreaSign(b, cc, h, Eps.GenerationFactor);
                    var s2 = m_eps.AreaSign(cc, a, h, Eps.GenerationFactor);
                    if (s0 == Sign3.On || s1 == Sign3.On || s2 == Sign3.On) { grazed = true; break; }
                    var inside = (s0 != Sign3.Below && s1 != Sign3.Below && s2 != Sign3.Below)
                              || (s0 != Sign3.Above && s1 != Sign3.Above && s2 != Sign3.Above);
                    if (!inside) continue;
                    w += denom > 0 ? 1 : -1;
                }
                if (!grazed) return w;
            }
            return null;
        }

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
                var ff = Fun.Max(m_kernel.TolFactor[frag.V0], m_kernel.TolFactor[frag.V1], m_kernel.TolFactor[frag.V2])
                    .Max(Eps.GenerationFactor);
                foreach (var (partner, same, slab) in partners)
                {
                    var pm = m_kernel.TriMesh[partner];
                    if (m_rel[f * n + pm] != 0) continue;
                    // coverage at the pair's working factor: grazing sheets
                    // scatter sliver centroids beyond the baseline band
                    var pf = Fun.Max(slab, ff)
                        .Max(m_kernel.TolFactor[m_kernel.T0[partner]])
                        .Max(m_kernel.TolFactor[m_kernel.T1[partner]])
                        .Max(m_kernel.TolFactor[m_kernel.T2[partner]]);
                    var covers = CoplanarCovers(partner, centroid, pf);
                    if (s_debugFrag == f.ToString())
                        Console.WriteLine($"FRAG {f}: partner {partner} mesh {pm} same {same} pf {pf:0.#} covers {covers}");
                    if (!covers) continue;
                    m_rel[f * n + pm] = (byte)(same ? FragLabel.OnSame : FragLabel.OnOpposite);
                }
                if (s_debugFrag == f.ToString() && partners.Count == 0)
                    Console.WriteLine($"FRAG {f}: no partners");
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
            if (Environment.GetEnvironmentVariable("CSG_DEBUG_REGIONS") != null)
                Console.WriteLine($"REGIONS: {regions.Count} sizes {string.Join(",", System.Linq.Enumerable.Select(System.Linq.Enumerable.Take(regions, 20), r => r.Count))}");
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
                    if (Environment.GetEnvironmentVariable("CSG_DEBUG_REGIONS") != null)
                        Console.WriteLine($"REGION {ri} mesh {mi} size {region.Count} vs {m}: {(FragLabel)pre}");
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
        private bool CoplanarCovers(int partner, in V3d p, double factor = Eps.GenerationFactor)
        {
            var n = m_kernel.Planes[m_kernel.TriPlane[partner]].Normal;
            Span<V2d> t = stackalloc V2d[3];
            t[0] = Triangulator.ProjectDominant(n, m_kernel.Positions[m_kernel.T0[partner]]);
            t[1] = Triangulator.ProjectDominant(n, m_kernel.Positions[m_kernel.T1[partner]]);
            t[2] = Triangulator.ProjectDominant(n, m_kernel.Positions[m_kernel.T2[partner]]);
            if (!MakeCcw(t)) return false;
            var q = Triangulator.ProjectDominant(n, p);
            return m_eps.AreaSign(t[0], t[1], q, factor) != Sign3.Below
                && m_eps.AreaSign(t[1], t[2], q, factor) != Sign3.Below
                && m_eps.AreaSign(t[2], t[0], q, factor) != Sign3.Below;
        }

        private bool RegionIsInsideOther(List<int> region, int otherMesh)
        {
            // try region fragments in order; per fragment try the ray directions:
            // any unambiguous parity decides
            foreach (var f in region)
            {
                var frag = Fragments[f];
                var o = (m_kernel.Positions[frag.V0] + m_kernel.Positions[frag.V1] + m_kernel.Positions[frag.V2]) / 3.0;
                var of = Fun.Max(m_kernel.TolFactor[frag.V0], m_kernel.TolFactor[frag.V1], m_kernel.TolFactor[frag.V2])
                    .Max(Eps.GenerationFactor);
                foreach (var dir in s_rayDirs)
                {
                    var parity = RayParity(o, dir, otherMesh, of);
                    if (parity.HasValue) return parity.Value;
                }
            }
            throw new CsgVerificationException("could not classify a surface region (all ray casts ambiguous)");
        }

        /// <summary>Parity of ray/other-mesh crossings; null when any hit is eps-ambiguous. BVH-accelerated.</summary>
        private bool? RayParity(V3d o, V3d dir, int otherMesh, double originFactor = Eps.GenerationFactor)
        {
            var count = 0;
            foreach (var t in m_bvh[otherMesh].RayCandidates(o, dir, m_triOf[otherMesh]))
            {
                var plane = m_kernel.Planes[m_kernel.TriPlane[t]];
                // a hit near the origin on grazing geometry poisons parity at
                // the WIDENED tolerances, not just the baseline band
                var tf = originFactor.Max(m_kernel.TolFactor[m_kernel.T0[t]])
                    .Max(m_kernel.TolFactor[m_kernel.T1[t]]).Max(m_kernel.TolFactor[m_kernel.T2[t]]);
                var denom = plane.Normal.Dot(dir);
                var h = plane.Normal.Dot(o) - plane.Distance;
                if (denom.Abs() < 1e-9)
                {
                    if (m_eps.HeightSign(plane, o, tf) == Sign3.On) return null; // ray (nearly) in plane near origin
                    continue;
                }
                var s = -h / denom;
                if (s <= 0) continue;
                var hit = o + s * dir;
                if (m_eps.AreCoincident(hit, o, tf)) return null; // origin on the other surface

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
