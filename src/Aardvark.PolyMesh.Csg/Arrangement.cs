using System;
using System.Collections.Generic;
using System.Linq;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    internal static class CsgDictExtensions
    {
        public static TV GetOrCreate<TK, TV>(this Dictionary<TK, TV> dict, TK key, Func<TK, TV> create)
            where TK : notnull
        {
            if (!dict.TryGetValue(key, out var v)) { v = create(key); dict[key] = v; }
            return v;
        }

        public static TV? GetOrDefault<TK, TV>(this Dictionary<TK, TV> dict, TK key)
            where TK : notnull where TV : class
            => dict.TryGetValue(key, out var v) ? v : null;
    }

    /// <summary>
    /// The arranged form of N input solids: all meshes ingested into the
    /// kernel, all mutual intersections resolved into fragments, and every
    /// fragment classified against every other solid. The boolean operations
    /// are cheap selections over this shared arrangement.
    /// </summary>
    public sealed class CsgArrangement
    {
        private readonly Kernel m_kernel;
        private readonly Pipeline m_pipeline;
        private readonly PolyMesh[] m_sources;
        private readonly CsgOptions m_options;

        private CsgArrangement(Kernel kernel, Pipeline pipeline, PolyMesh[] sources, CsgOptions options)
        {
            m_kernel = kernel;
            m_pipeline = pipeline;
            m_sources = sources;
            m_options = options;
        }

        /// <summary>True if any two solids have coincident (coplanar) surface regions.</summary>
        public bool HasCoincidentContact => m_pipeline.HasCoincidentContact;

        public int SolidCount => m_sources.Length;

        public static CsgArrangement Arrange(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => Arrange(new[] { a, b }, options);

        /// <summary>Arranges N solids at once — one kernel, one set of BVHs, one subdivision.</summary>
        public static CsgArrangement Arrange(PolyMesh[] solids, CsgOptions? options = null)
        {
            if (solids.Length < 2) throw new ArgumentException("need at least two solids");
            var o = options ?? CsgOptions.Default;
            if (o.Verification != CsgVerification.None)
            {
                // input verification per solid, in parallel
                var violations = new string?[solids.Length];
                CsgParallel.For(0, solids.Length, o.MaxThreads, i =>
                    violations[i] = ManifoldChecks.FindManifoldViolation(
                        solids[i].FirstIndexArray, solids[i].VertexIndexArray, solids[i].PositionArray.Length));
                for (var i = 0; i < solids.Length; i++)
                    if (violations[i] != null)
                        throw new CsgInputException($"input mesh {i} is not a closed manifold: {violations[i]}");
            }
            var kernel = new Kernel(new Eps(o.RelativeEpsilon));
            for (var i = 0; i < solids.Length; i++) kernel.Ingest(solids[i], i, verify: false);
            var pipeline = new Pipeline(kernel, maxThreads: o.MaxThreads);
            pipeline.Run();
            return new CsgArrangement(kernel, pipeline, solids, o);
        }

        public static CsgArrangement Arrange(CsgMesh a, CsgMesh b, CsgOptions? options = null)
            => Arrange(new[] { a, b }, options);

        /// <summary>
        /// Arranges N prepared solids: no re-verification, no re-triangulation,
        /// planes carried through, cached BVHs reused.
        /// </summary>
        public static CsgArrangement Arrange(CsgMesh[] solids, CsgOptions? options = null)
        {
            if (solids.Length < 2) throw new ArgumentException("need at least two solids");
            var o = options ?? CsgOptions.Default;
            var kernel = new Kernel(new Eps(o.RelativeEpsilon));
            for (var i = 0; i < solids.Length; i++) kernel.IngestPrepared(solids[i], i);
            var pipeline = new Pipeline(kernel, solids, o.MaxThreads);
            pipeline.Run();
            return new CsgArrangement(kernel, pipeline, solids.Map(s => s.Source), o);
        }

        /// <summary>Union as prepared solids (one per component).</summary>
        public CsgMesh[] UnionSolids() => EmitSolids(Union);
        /// <summary>Intersection as prepared solids (one per component).</summary>
        public CsgMesh[] IntersectionSolids() => EmitSolids(Intersection);
        /// <summary>Difference (solid 0 minus the rest) as prepared solids.</summary>
        public CsgMesh[] DifferenceSolids() => EmitSolids(Difference);

        private CsgMesh[] m_lastSolids = Array.Empty<CsgMesh>();

        private CsgMesh[] EmitSolids(Func<PolyMesh[]> op)
        {
            op(); // Emitter records CsgMesh results alongside the PolyMeshes
            return m_lastSolids;
        }

        // Coincident (coplanar) surface regions exist once in each covering
        // solid; selections keep the copy of the lowest-indexed solid so the
        // region is emitted exactly once.

        /// <summary>Boundary of the union of all solids.</summary>
        public PolyMesh[] Union() => Emit((mi, f) =>
        {
            for (var m = 0; m < SolidCount; m++)
            {
                if (m == mi) continue;
                switch (m_pipeline.Label(f, m))
                {
                    case FragLabel.Outside: break;
                    case FragLabel.Inside: return Selection.Drop;
                    case FragLabel.OnSame: if (m < mi) return Selection.Drop; break;
                    case FragLabel.OnOpposite: return Selection.Drop;
                    default: throw new InvalidOperationException();
                }
            }
            return Selection.Keep;
        });

        /// <summary>Boundary of the intersection of all solids.</summary>
        public PolyMesh[] Intersection() => Emit((mi, f) =>
        {
            for (var m = 0; m < SolidCount; m++)
            {
                if (m == mi) continue;
                switch (m_pipeline.Label(f, m))
                {
                    case FragLabel.Outside: return Selection.Drop;
                    case FragLabel.Inside: break;
                    case FragLabel.OnSame: if (m < mi) return Selection.Drop; break;
                    case FragLabel.OnOpposite: return Selection.Drop;
                    default: throw new InvalidOperationException();
                }
            }
            return Selection.Keep;
        });

        /// <summary>Boundary of solid 0 minus the union of all others.</summary>
        public PolyMesh[] Difference() => Emit((mi, f) =>
        {
            if (mi == 0)
            {
                // survive iff no subtrahend covers or contains this piece
                for (var m = 1; m < SolidCount; m++)
                {
                    switch (m_pipeline.Label(f, m))
                    {
                        case FragLabel.Outside: case FragLabel.OnOpposite: break;
                        case FragLabel.Inside: case FragLabel.OnSame: return Selection.Drop;
                        default: throw new InvalidOperationException();
                    }
                }
                return Selection.Keep;
            }
            // subtrahend boundary: carved wall iff inside the minuend and not
            // absorbed by any other subtrahend
            if (m_pipeline.Label(f, 0) != FragLabel.Inside) return Selection.Drop;
            for (var m = 1; m < SolidCount; m++)
            {
                if (m == mi) continue;
                switch (m_pipeline.Label(f, m))
                {
                    case FragLabel.Outside: break;
                    case FragLabel.OnSame: if (m < mi) return Selection.Drop; break;
                    case FragLabel.Inside: case FragLabel.OnOpposite: return Selection.Drop;
                    default: throw new InvalidOperationException();
                }
            }
            return Selection.Flip;
        });

        /// <summary>
        /// Symmetric difference (two solids only), emitted as the two lobes
        /// A∖B and B∖A. They touch along the intersection curve, where a single
        /// merged surface would be non-manifold — separate solids keep the
        /// manifold guarantee.
        /// </summary>
        public PolyMesh[] Xor() => XorSolids().Map(s => s.ToPolyMesh());

        /// <summary>Symmetric difference as prepared solids (both lobes' components).</summary>
        public CsgMesh[] XorSolids()
        {
            if (SolidCount != 2) throw new NotSupportedException("Xor is defined for two solids");
            Difference();
            var first = m_lastSolids;
            Emit((mi, f) =>
            {
                if (mi == 1)
                    return m_pipeline.Label(f, 0) switch
                    {
                        FragLabel.Outside or FragLabel.OnOpposite => Selection.Keep,
                        _ => Selection.Drop,
                    };
                return m_pipeline.Label(f, 1) == FragLabel.Inside ? Selection.Flip : Selection.Drop;
            });
            return first.Concat(m_lastSolids).ToArray();
        }

        private enum Selection { Drop, Keep, Flip }

        private PolyMesh[] Emit(Func<int, int, Selection> select)
        {
            var tris = new List<EmitTri>();
            for (var f = 0; f < m_pipeline.Fragments.Count; f++)
            {
                var frag = m_pipeline.Fragments[f];
                var mesh = m_kernel.TriMesh[frag.Parent];
                switch (select(mesh, f))
                {
                    case Selection.Drop: break;
                    case Selection.Keep: tris.Add(new EmitTri(frag.V0, frag.V1, frag.V2, frag.Parent)); break;
                    case Selection.Flip: tris.Add(new EmitTri(frag.V0, frag.V2, frag.V1, frag.Parent)); break;
                    default: throw new InvalidOperationException();
                }
            }
            var solids = Emitter.Emit(m_kernel, tris, m_sources,
                m_options.Verification == CsgVerification.Full, m_options.MaxThreads);
            m_lastSolids = solids;
            return solids.Map(s => s.ToPolyMesh());
        }
    }

    internal readonly struct EmitTri
    {
        public readonly int V0, V1, V2;
        public readonly int Parent;
        public EmitTri(int v0, int v1, int v2, int parent) { V0 = v0; V1 = v1; V2 = v2; Parent = parent; }
    }

    /// <summary>
    /// Builds output PolyMeshes from selected fragments: one PolyMesh per
    /// edge-connected component. Attribute values at derived (cut) vertices are
    /// synthesized by barycentric interpolation over the parent triangle —
    /// evaluated once per output vertex, so interpolated channels cannot crack
    /// across fragment seams.
    /// </summary>
    internal static class Emitter
    {
        public static CsgMesh[] Emit(Kernel k, List<EmitTri> tris, PolyMesh[] sources, bool verify, int maxThreads = 1)
        {
            if (tris.Count == 0) return Array.Empty<CsgMesh>();
            var sw = Environment.GetEnvironmentVariable("CSG_PERF") != null
                ? System.Diagnostics.Stopwatch.StartNew() : null;

            // 1. halfedge pairing: normal edges pair their two faces; at edges
            //    where more triangles meet (result volumes touching along a
            //    curve), faces are paired by dihedral angle into solid wedges
            var pair = PairHalfedges(k, tris);

            // 2. components = connectivity through paired halfedges
            var componentOfFace = new int[tris.Count].Set(-1);
            var componentCount = 0;
            var stack = new Stack<int>();
            for (var seed = 0; seed < tris.Count; seed++)
            {
                if (componentOfFace[seed] >= 0) continue;
                var ci = componentCount++;
                componentOfFace[seed] = ci;
                stack.Push(seed);
                while (stack.Count > 0)
                {
                    var i = stack.Pop();
                    for (var slot = 0; slot < 3; slot++)
                    {
                        var p = pair[i * 3 + slot].Tri;
                        if (p >= 0 && componentOfFace[p] < 0) { componentOfFace[p] = ci; stack.Push(p); }
                    }
                }
            }

            // 3. manifold sheet extraction: group each kernel vertex's incident
            //    corners by link-connectivity through paired halfedges and emit
            //    one output vertex per group — a solid pinched along an edge or
            //    at a vertex (self-touching result) becomes combinatorially
            //    manifold with geometrically coincident vertices; for clean
            //    meshes this reduces to plain vertex compaction
            var cornerGroup = new int[tris.Count * 3].SetByIndex(i => i);
            int Find(int i) { while (cornerGroup[i] != i) { cornerGroup[i] = cornerGroup[cornerGroup[i]]; i = cornerGroup[i]; } return i; }
            void Union(int i, int j)
            {
                var ri = Find(i); var rj = Find(j);
                if (ri != rj) cornerGroup[ri.Max(rj)] = ri.Min(rj);
            }
            static int Corner(EmitTri t, int c) => c == 0 ? t.V0 : c == 1 ? t.V1 : t.V2;
            static int CornerOf(EmitTri t, int vid) => t.V0 == vid ? 0 : t.V1 == vid ? 1 : 2;
            for (var i = 0; i < tris.Count; i++)
            {
                for (var slot = 0; slot < 3; slot++)
                {
                    var (pt, _) = pair[i * 3 + slot];
                    if (pt < 0) continue;
                    var u = Corner(tris[i], slot);
                    var v = Corner(tris[i], (slot + 1) % 3);
                    Union(i * 3 + slot, pt * 3 + CornerOf(tris[pt], u));
                    Union(i * 3 + (slot + 1) % 3, pt * 3 + CornerOf(tris[pt], v));
                }
            }

            if (sw != null) { Console.WriteLine($"PERF emit-pairing: {sw.Elapsed.TotalMilliseconds:0.0} ms"); sw.Restart(); }
            var result = new CsgMesh[componentCount];
            for (var ci = 0; ci < componentCount; ci++)
            {
                var componentTris = new List<int>();
                for (var i = 0; i < tris.Count; i++)
                    if (componentOfFace[i] == ci) componentTris.Add(i);
                result[ci] = BuildSolid(k, tris, componentTris, Find, sources, verify);
            }
            if (sw != null) Console.WriteLine($"PERF emit-build+verify ({tris.Count} tris): {sw.Elapsed.TotalMilliseconds:0.0} ms");
            return result;
        }

        /// <summary>
        /// Per halfedge (tri, edge slot): the paired (tri, slot) across that
        /// edge, or (-1,-1). Multi-edges are paired by dihedral angle: sorted
        /// CCW around the edge axis, a face traversing v→u followed by one
        /// traversing u→v bound one solid wedge. Angle ties are coplanar
        /// continuations and ordered v→u first so they pair like a zero-angle
        /// wedge.
        /// </summary>
        private static (int Tri, int Slot)[] PairHalfedges(Kernel k, List<EmitTri> tris)
        {
            var pair = new (int Tri, int Slot)[tris.Count * 3].Set((-1, -1));
            static int Corner(EmitTri t, int c) => c == 0 ? t.V0 : c == 1 ? t.V1 : t.V2;

            // one sort instead of a tuple-keyed dictionary of lists
            var keys = new long[tris.Count * 3];
            var hs = new int[tris.Count * 3];
            for (var i = 0; i < tris.Count; i++)
                for (var slot = 0; slot < 3; slot++)
                {
                    keys[i * 3 + slot] = Pipeline.EdgeKey(Corner(tris[i], slot), Corner(tris[i], (slot + 1) % 3));
                    hs[i * 3 + slot] = i * 3 + slot;
                }
            RadixSorter.SortEdgeKeys(keys, hs, keys.Length);

            void Pair((int Tri, int Slot) a, (int Tri, int Slot) b)
            {
                pair[a.Tri * 3 + a.Slot] = b;
                pair[b.Tri * 3 + b.Slot] = a;
            }

            var list = new List<(int Tri, int Slot)>(8);
            for (var gi = 0; gi < keys.Length;)
            {
                var gj = gi + 1;
                while (gj < keys.Length && keys[gj] == keys[gi]) gj++;
                list.Clear();
                for (var x = gi; x < gj; x++) list.Add((hs[x] / 3, hs[x] % 3));
                var groupKey = keys[gi];
                gi = gj;

                if (list.Count == 2)
                {
                    Pair(list[0], list[1]);
                }
                else if (list.Count > 2)
                {
                    var u = (int)(groupKey >> 32); var v = (int)groupKey;
                    var d = (k.Positions[v] - k.Positions[u]).Normalized;
                    var ax0 = d.X.Abs() < 0.9 ? V3d.XAxis : V3d.YAxis;
                    var ax1 = d.Cross(ax0).Normalized;
                    var ax2 = d.Cross(ax1);
                    var around = list.Map(h =>
                    {
                        var t = tris[h.Tri];
                        var w = t.V0 != u && t.V0 != v ? t.V0 : t.V1 != u && t.V1 != v ? t.V1 : t.V2;
                        var r = k.Positions[w] - k.Positions[u];
                        var angle = Fun.Atan2(r.Dot(ax2), r.Dot(ax1));
                        var forward = Corner(tris[h.Tri], h.Slot) == u;
                        return (H: h, Angle: angle, Forward: forward);
                    }).ToArray();
                    Array.Sort(around, (x, y) =>
                        (x.Angle - y.Angle).Abs() < 1e-9
                            ? x.Forward.CompareTo(y.Forward)
                            : x.Angle.CompareTo(y.Angle));
                    for (var i = 0; i < around.Length; i++)
                    {
                        var a = around[i];
                        var b = around[(i + 1) % around.Length];
                        if (!a.Forward && b.Forward) Pair(a.H, b.H);
                        // other consecutive combinations bound void sectors, or
                        // indicate broken geometry (the verifier reports those)
                    }
                }
            }
            return pair;
        }

        private static CsgMesh BuildSolid(
            Kernel k, List<EmitTri> allTris, List<int> triIndices, Func<int, int> cornerGroupOf,
            PolyMesh[] sources, bool verify)
        {
            var sw = Environment.GetEnvironmentVariable("CSG_PERF") != null
                ? System.Diagnostics.Stopwatch.StartNew() : null;
            void Lap(string what)
            {
                if (sw == null) return;
                Console.WriteLine($"PERF   build-{what}: {sw.Elapsed.TotalMilliseconds:0.0} ms");
                sw.Restart();
            }
            var triCount = triIndices.Count;
            var tris = triIndices.Map(i => allTris[i]).ToList();

            // one output vertex per corner group
            var localOfGroup = new Dictionary<int, int>();
            var kernelOfLocal = new List<int>();
            var via = new int[triCount * 3];
            var fia = new int[triCount + 1];
            for (var i = 0; i < triCount; i++)
            {
                var ti = triIndices[i];
                fia[i + 1] = (i + 1) * 3;
                for (var c = 0; c < 3; c++)
                {
                    var group = cornerGroupOf(ti * 3 + c);
                    if (!localOfGroup.TryGetValue(group, out var li))
                    {
                        li = kernelOfLocal.Count;
                        localOfGroup[group] = li;
                        kernelOfLocal.Add(c == 0 ? tris[i].V0 : c == 1 ? tris[i].V1 : tris[i].V2);
                    }
                    via[i * 3 + c] = li;
                }
            }

            Lap("compact");
            var positions = new V3d[kernelOfLocal.Count];
            for (var li = 0; li < positions.Length; li++)
                positions[li] = k.Positions[kernelOfLocal[li]];

            // one representative (parent, barycentric) per output vertex for
            // channel synthesis — only when vertex channels will be emitted
            var vertexDicts = sources.Map(src => src.VertexAttributes);
            var hasVertexChannels = CommonChannels(vertexDicts, PolyMesh.Property.Positions).Any();
            var repParent = Array.Empty<int>();
            var repBary = Array.Empty<V3d>();
            if (hasVertexChannels)
            {
                repParent = new int[kernelOfLocal.Count].Set(-1);
                repBary = new V3d[kernelOfLocal.Count];
                for (var i = 0; i < triCount; i++)
                {
                    for (var c = 0; c < 3; c++)
                    {
                        var li = via[i * 3 + c];
                        if (repParent[li] >= 0) continue;
                        repParent[li] = tris[i].Parent;
                        repBary[li] = Barycentric(k, tris[i].Parent, k.Positions[kernelOfLocal[li]]);
                    }
                }
            }

            Lap("rep");
            var mesh = new PolyMesh
            {
                PositionArray = positions,
                FirstIndexArray = fia,
                VertexIndexArray = via,
            };
            Lap("polymesh");

            EmitVertexAttributes(k, mesh, kernelOfLocal, repParent, repBary, sources);
            EmitFaceAttributes(k, mesh, tris, sources);
            EmitFaceVertexAttributes(k, mesh, tris, sources);
            EmitInstanceAttributes(k, mesh, tris, sources);
            Lap("attrs");

            if (verify)
            {
                var violation = ManifoldChecks.FindManifoldViolation(fia, via, positions.Length);
                if (violation != null)
                    throw new CsgVerificationException($"output verification failed: {violation}");
            }
            Lap("verify");

            // kernel form of the result: per-tri parent planes (ground truth
            // carried through chains), sequential face ids, identity slots
            var outT0 = new int[triCount]; var outT1 = new int[triCount]; var outT2 = new int[triCount];
            var outPlanes = new Plane3d[triCount];
            var outTriPlane = new int[triCount]; var outTriFace = new int[triCount];
            var outC0 = new int[triCount]; var outC1 = new int[triCount]; var outC2 = new int[triCount];
            for (var i = 0; i < triCount; i++)
            {
                outT0[i] = via[i * 3]; outT1[i] = via[i * 3 + 1]; outT2[i] = via[i * 3 + 2];
                var plane = k.Planes[k.TriPlane[tris[i].Parent]];
                // flipped fragments (difference walls) carry the negated plane:
                // plane orientation must match the output winding
                var wind = (positions[outT1[i]] - positions[outT0[i]])
                    .Cross(positions[outT2[i]] - positions[outT0[i]]);
                if (wind.Dot(plane.Normal) < 0) plane = new Plane3d(-plane.Normal, -plane.Distance);
                outPlanes[i] = plane;
                outTriPlane[i] = i;
                outTriFace[i] = i;
                outC0[i] = i * 3; outC1[i] = i * 3 + 1; outC2[i] = i * 3 + 2;
            }
            return new CsgMesh(mesh, outT0, outT1, outT2, outPlanes, outTriPlane, outTriFace, outC0, outC1, outC2,
                spatiallyOrdered: true);
        }

        /// <summary>Barycentric coordinates of p in the parent triangle, computed in the parent plane's 2D projection.</summary>
        private static V3d Barycentric(Kernel k, int parent, in V3d p)
        {
            var normal = k.Planes[k.TriPlane[parent]].Normal;
            var a = Triangulator.ProjectDominant(normal, k.Positions[k.T0[parent]]);
            var b = Triangulator.ProjectDominant(normal, k.Positions[k.T1[parent]]);
            var c = Triangulator.ProjectDominant(normal, k.Positions[k.T2[parent]]);
            var q = Triangulator.ProjectDominant(normal, p);
            var area = Det(b - a, c - a);
            if (area == 0.0) return new V3d(1, 0, 0);
            var w0 = Det(b - q, c - q) / area;
            var w1 = Det(c - q, a - q) / area;
            return new V3d(w0, w1, 1.0 - w0 - w1);
        }

        private static double Det(in V2d u, in V2d v) => u.X * v.Y - u.Y * v.X;

        /// <summary>Weighted combination of three source-array entries; falls back to nearest for non-interpolatable types.</summary>
        private static object BaryValue(Array src, int i0, int i1, int i2, in V3d w, bool normalize)
        {
            switch (src)
            {
                case double[] a: return w.X * a[i0] + w.Y * a[i1] + w.Z * a[i2];
                case float[] a: return (float)(w.X * a[i0] + w.Y * a[i1] + w.Z * a[i2]);
                case V2d[] a: return w.X * a[i0] + w.Y * a[i1] + w.Z * a[i2];
                case V3d[] a:
                {
                    var v = w.X * a[i0] + w.Y * a[i1] + w.Z * a[i2];
                    return normalize && v != V3d.Zero ? v.Normalized : v;
                }
                case V4d[] a: return w.X * a[i0] + w.Y * a[i1] + w.Z * a[i2];
                case V2f[] a: return (float)w.X * a[i0] + (float)w.Y * a[i1] + (float)w.Z * a[i2];
                case V3f[] a:
                {
                    var v = (float)w.X * a[i0] + (float)w.Y * a[i1] + (float)w.Z * a[i2];
                    return normalize && v != V3f.Zero ? v.Normalized : v;
                }
                case V4f[] a: return (float)w.X * a[i0] + (float)w.Y * a[i1] + (float)w.Z * a[i2];
                case C3f[] a: return new C3f(
                    (float)(w.X * a[i0].R + w.Y * a[i1].R + w.Z * a[i2].R),
                    (float)(w.X * a[i0].G + w.Y * a[i1].G + w.Z * a[i2].G),
                    (float)(w.X * a[i0].B + w.Y * a[i1].B + w.Z * a[i2].B));
                case C4f[] a: return new C4f(
                    (float)(w.X * a[i0].R + w.Y * a[i1].R + w.Z * a[i2].R),
                    (float)(w.X * a[i0].G + w.Y * a[i1].G + w.Z * a[i2].G),
                    (float)(w.X * a[i0].B + w.Y * a[i1].B + w.Z * a[i2].B),
                    (float)(w.X * a[i0].A + w.Y * a[i1].A + w.Z * a[i2].A));
                default:
                {
                    var nearest = w.X >= w.Y && w.X >= w.Z ? i0 : w.Y >= w.Z ? i1 : i2;
                    return src.GetValue(nearest)!;
                }
            }
        }

        /// <summary>Channels usable for output: present in every source with the same element type, non-indexed everywhere.</summary>
        private static IEnumerable<(Symbol Name, Array[] Arrays)> CommonChannels(
            SymbolDict<Array>[] dicts, Symbol skip = default)
        {
            foreach (var name in dicts[0].Keys.ToArray())
            {
                if (!name.IsPositive || name == skip) continue;
                var arrays = new Array[dicts.Length];
                var ok = true;
                for (var m = 0; m < dicts.Length && ok; m++)
                {
                    ok = !dicts[m].Contains(-name)
                        && dicts[m].TryGetValue(name, out var a) && a != null
                        && a.GetType().GetElementType() == dicts[0][name].GetType().GetElementType();
                    if (ok) arrays[m] = dicts[m][name];
                }
                if (ok) yield return (name, arrays);
            }
        }

        private static void EmitVertexAttributes(
            Kernel k, PolyMesh mesh, List<int> kernelOfLocal, int[] repParent, V3d[] repBary, PolyMesh[] sources)
        {
            var dicts = sources.Map(src => src.VertexAttributes);
            foreach (var (name, arrays) in CommonChannels(dicts, PolyMesh.Property.Positions))
            {
                var elementType = arrays[0].GetType().GetElementType()!;
                var normalize = name == PolyMesh.Property.Normals;
                var target = Array.CreateInstance(elementType, kernelOfLocal.Count);
                for (var li = 0; li < kernelOfLocal.Count; li++)
                {
                    var parent = repParent[li];
                    var mi = k.TriMesh[parent];
                    var src = sources[mi];
                    var i0 = src.VertexIndexArray[k.C0[parent]];
                    var i1 = src.VertexIndexArray[k.C1[parent]];
                    var i2 = src.VertexIndexArray[k.C2[parent]];
                    target.SetValue(BaryValue(arrays[mi], i0, i1, i2, repBary[li], normalize), li);
                }
                mesh.VertexAttributes[name] = target;
            }
        }

        private static void EmitFaceAttributes(
            Kernel k, PolyMesh mesh, List<EmitTri> tris, PolyMesh[] sources)
        {
            var dicts = sources.Map(src => src.FaceAttributes);
            foreach (var (name, arrays) in CommonChannels(dicts))
            {
                var elementType = arrays[0].GetType().GetElementType()!;
                var target = Array.CreateInstance(elementType, tris.Count);
                for (var i = 0; i < tris.Count; i++)
                {
                    var parent = tris[i].Parent;
                    target.SetValue(arrays[k.TriMesh[parent]].GetValue(k.TriFace[parent]), i);
                }
                mesh.FaceAttributes[name] = target;
            }
        }

        private static void EmitFaceVertexAttributes(
            Kernel k, PolyMesh mesh, List<EmitTri> tris, PolyMesh[] sources)
        {
            // face-vertex channels may be indexed (name = values + -name = index)
            // or per-slot; output is emitted in indexed form over all sources'
            // concatenated value arrays plus synthesized cut values
            var dicts = sources.Map(src => src.FaceVertexAttributes);
            foreach (var name in dicts[0].Keys.ToArray())
            {
                if (!name.IsPositive) continue;
                var values = new Array[sources.Length];
                var idx = new int[sources.Length][];
                var offsets = new int[sources.Length];
                var elementType = default(Type);
                var ok = true;
                var baseLength = 0;
                for (var m = 0; m < sources.Length && ok; m++)
                {
                    ok = dicts[m].TryGetValue(name, out var v) && v != null;
                    if (!ok) break;
                    elementType ??= v!.GetType().GetElementType();
                    ok = elementType != null && elementType == v!.GetType().GetElementType();
                    if (!ok) break;
                    values[m] = v!;
                    idx[m] = dicts[m].GetOrDefault(-name) as int[];
                    offsets[m] = baseLength;
                    baseLength += v!.Length;
                }
                if (!ok) continue;
                var normalize = name == PolyMesh.Property.Normals;

                var extra = new List<object>();
                var indices = new int[tris.Count * 3];
                for (var i = 0; i < tris.Count; i++)
                {
                    var t = tris[i];
                    var parent = t.Parent;
                    var mi = k.TriMesh[parent];
                    var offset = offsets[mi];
                    int SlotValueIndex(int slot) => idx[mi] != null ? idx[mi][slot] : slot;

                    Span<int> corner = stackalloc int[] { t.V0, t.V1, t.V2 };
                    for (var c = 0; c < 3; c++)
                    {
                        int at;
                        if (corner[c] == k.T0[parent]) at = offset + SlotValueIndex(k.C0[parent]);
                        else if (corner[c] == k.T1[parent]) at = offset + SlotValueIndex(k.C1[parent]);
                        else if (corner[c] == k.T2[parent]) at = offset + SlotValueIndex(k.C2[parent]);
                        else
                        {
                            var w = BarycentricOf(k, parent, k.Positions[corner[c]]);
                            extra.Add(BaryValue(values[mi],
                                SlotValueIndex(k.C0[parent]), SlotValueIndex(k.C1[parent]), SlotValueIndex(k.C2[parent]),
                                w, normalize));
                            at = baseLength + extra.Count - 1;
                        }
                        indices[i * 3 + c] = at;
                    }
                }

                var all = Array.CreateInstance(elementType!, baseLength + extra.Count);
                for (var m = 0; m < sources.Length; m++)
                    Array.Copy(values[m], 0, all, offsets[m], values[m].Length);
                for (var e = 0; e < extra.Count; e++) all.SetValue(extra[e], baseLength + e);

                mesh.FaceVertexAttributes[name] = all;
                mesh.FaceVertexAttributes[-name] = indices;
            }
        }

        private static V3d BarycentricOf(Kernel k, int parent, in V3d p) => Barycentric(k, parent, p);

        private static void EmitInstanceAttributes(
            Kernel k, PolyMesh mesh, List<EmitTri> tris, PolyMesh[] sources)
        {
            var mi = k.TriMesh[tris[0].Parent];
            for (var i = 1; i < tris.Count; i++)
                if (k.TriMesh[tris[i].Parent] != mi) { mi = 0; break; }
            foreach (var name in sources[mi].InstanceAttributes.Keys.ToArray())
                mesh.InstanceAttributes[name] = sources[mi].InstanceAttributes[name];
        }
    }
}
