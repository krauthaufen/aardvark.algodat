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
    /// The arranged form of two input solids: both meshes ingested into the
    /// kernel, all mutual intersections resolved into fragments, and every
    /// fragment labeled inside/outside the other solid. The boolean operations
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

        /// <summary>True if the two solids have coincident (coplanar) surface regions.</summary>
        public bool HasCoincidentContact => m_pipeline.Labels.Any(
            l => l == FragLabel.OnSame || l == FragLabel.OnOpposite);

        public static CsgArrangement Arrange(PolyMesh a, PolyMesh b, CsgOptions? options = null)
        {
            var o = options ?? CsgOptions.Default;
            if (o.Verification == CsgVerification.None)
                throw new NotSupportedException("verification cannot be disabled in v0");
            var kernel = new Kernel(new Eps(o.RelativeEpsilon));
            kernel.Ingest(a, 0);
            kernel.Ingest(b, 1);
            var pipeline = new Pipeline(kernel);
            pipeline.Run();
            return new CsgArrangement(kernel, pipeline, new[] { a, b }, o);
        }

        // Coincident (coplanar) surface regions exist once in each input; the
        // selection keeps A's copy when the region belongs to the result
        // (OnSame for union/intersection) and drops B's, so the region is
        // emitted exactly once.

        /// <summary>Fragments of each solid outside the other; coincident same-facing regions kept once.</summary>
        public PolyMesh[] Union() => Emit((mesh, label) => label switch
        {
            FragLabel.Outside => Selection.Keep,
            FragLabel.Inside => Selection.Drop,
            FragLabel.OnSame => mesh == 0 ? Selection.Keep : Selection.Drop,
            FragLabel.OnOpposite => Selection.Drop,
            _ => throw new InvalidOperationException(),
        });

        /// <summary>Fragments of each solid inside the other; coincident same-facing regions kept once.</summary>
        public PolyMesh[] Intersection() => Emit((mesh, label) => label switch
        {
            FragLabel.Outside => Selection.Drop,
            FragLabel.Inside => Selection.Keep,
            FragLabel.OnSame => mesh == 0 ? Selection.Keep : Selection.Drop,
            FragLabel.OnOpposite => Selection.Drop,
            _ => throw new InvalidOperationException(),
        });

        /// <summary>A∖B: A outside B (plus A's faces where B touches from outside), B inside A flipped.</summary>
        public PolyMesh[] Difference() => Emit(DifferenceSelect(0));

        /// <summary>
        /// Symmetric difference, emitted as the two lobes A∖B and B∖A. They
        /// touch along the intersection curve, where a single merged surface
        /// would be non-manifold — separate solids keep the manifold guarantee.
        /// </summary>
        public PolyMesh[] Xor() => Difference().Concat(Emit(DifferenceSelect(1))).ToArray();

        private static Func<int, FragLabel, Selection> DifferenceSelect(int keptMesh) => (mesh, label) =>
        {
            if (mesh == keptMesh)
                return label switch
                {
                    FragLabel.Outside => Selection.Keep,
                    FragLabel.Inside => Selection.Drop,
                    FragLabel.OnSame => Selection.Drop,      // covered by the subtrahend from the same side
                    FragLabel.OnOpposite => Selection.Keep,  // subtrahend only touches from outside
                    _ => throw new InvalidOperationException(),
                };
            return label switch
            {
                FragLabel.Outside => Selection.Drop,
                FragLabel.Inside => Selection.Flip,
                FragLabel.OnSame => Selection.Drop,
                FragLabel.OnOpposite => Selection.Drop,
                _ => throw new InvalidOperationException(),
            };
        };

        private enum Selection { Drop, Keep, Flip }

        private PolyMesh[] Emit(Func<int, FragLabel, Selection> select)
        {
            var tris = new List<EmitTri>();
            for (var f = 0; f < m_pipeline.Fragments.Count; f++)
            {
                var frag = m_pipeline.Fragments[f];
                var mesh = m_kernel.TriMesh[frag.Parent];
                switch (select(mesh, m_pipeline.Labels[f]))
                {
                    case Selection.Drop: break;
                    case Selection.Keep: tris.Add(new EmitTri(frag.V0, frag.V1, frag.V2, frag.Parent)); break;
                    case Selection.Flip: tris.Add(new EmitTri(frag.V0, frag.V2, frag.V1, frag.Parent)); break;
                    default: throw new InvalidOperationException();
                }
            }
            return Emitter.Emit(m_kernel, tris, m_sources, m_options.Verification == CsgVerification.Full);
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
        public static PolyMesh[] Emit(Kernel k, List<EmitTri> tris, PolyMesh[] sources, bool verify)
        {
            if (tris.Count == 0) return Array.Empty<PolyMesh>();

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

            var result = new PolyMesh[componentCount];
            for (var ci = 0; ci < componentCount; ci++)
            {
                var componentTris = new List<int>();
                for (var i = 0; i < tris.Count; i++)
                    if (componentOfFace[i] == ci) componentTris.Add(i);
                result[ci] = BuildPolyMesh(k, tris, componentTris, Find, sources, verify);
            }
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
            var edgeTris = new Dictionary<(int, int), List<(int Tri, int Slot)>>();
            static (int, int) Key(int a, int b) => a < b ? (a, b) : (b, a);
            static int Corner(EmitTri t, int c) => c == 0 ? t.V0 : c == 1 ? t.V1 : t.V2;
            for (var i = 0; i < tris.Count; i++)
                for (var slot = 0; slot < 3; slot++)
                    edgeTris.GetOrCreate(Key(Corner(tris[i], slot), Corner(tris[i], (slot + 1) % 3)),
                        _ => new List<(int, int)>()).Add((i, slot));

            void Pair((int Tri, int Slot) a, (int Tri, int Slot) b)
            {
                pair[a.Tri * 3 + a.Slot] = b;
                pair[b.Tri * 3 + b.Slot] = a;
            }

            foreach (var (edge, list) in edgeTris)
            {
                if (list.Count == 2)
                {
                    Pair(list[0], list[1]);
                }
                else if (list.Count > 2)
                {
                    var (u, v) = edge;
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

        private static PolyMesh BuildPolyMesh(
            Kernel k, List<EmitTri> allTris, List<int> triIndices, Func<int, int> cornerGroupOf,
            PolyMesh[] sources, bool verify)
        {
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

            var positions = new V3d[kernelOfLocal.Count];
            for (var li = 0; li < positions.Length; li++)
                positions[li] = k.Positions[kernelOfLocal[li]];

            // one representative (parent, barycentric) per output vertex for
            // channel synthesis
            var repParent = new int[kernelOfLocal.Count].Set(-1);
            var repBary = new V3d[kernelOfLocal.Count];
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

            var mesh = new PolyMesh
            {
                PositionArray = positions,
                FirstIndexArray = fia,
                VertexIndexArray = via,
            };

            EmitVertexAttributes(k, mesh, kernelOfLocal, repParent, repBary, sources);
            EmitFaceAttributes(k, mesh, tris, sources);
            EmitFaceVertexAttributes(k, mesh, tris, sources);
            EmitInstanceAttributes(k, mesh, tris, sources);

            if (verify)
            {
                var violation = ManifoldChecks.FindManifoldViolation(fia, via, positions.Length);
                if (violation != null)
                    throw new CsgVerificationException($"output verification failed: {violation}");
            }
            return mesh;
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

        private static IEnumerable<(Symbol Name, Array[] Arrays)> CommonChannels(
            SymbolDict<Array>[] dicts, Symbol skip = default)
        {
            foreach (var name in dicts[0].Keys.ToArray())
            {
                if (!name.IsPositive || name == skip) continue;
                if (dicts[0].Contains(-name) || dicts[1].Contains(-name)) continue;
                if (!dicts[0].TryGetValue(name, out var a0) || a0 == null) continue;
                if (!dicts[1].TryGetValue(name, out var a1) || a1 == null) continue;
                if (a0.GetType().GetElementType() != a1.GetType().GetElementType()) continue;
                yield return (name, new[] { a0, a1 });
            }
        }

        private static void EmitVertexAttributes(
            Kernel k, PolyMesh mesh, List<int> kernelOfLocal, int[] repParent, V3d[] repBary, PolyMesh[] sources)
        {
            var dicts = new[] { sources[0].VertexAttributes, sources[1].VertexAttributes };
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
            var dicts = new[] { sources[0].FaceAttributes, sources[1].FaceAttributes };
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
            // or per-slot; output is emitted in indexed form over the two
            // sources' concatenated value arrays plus synthesized cut values
            var dicts = new[] { sources[0].FaceVertexAttributes, sources[1].FaceVertexAttributes };
            foreach (var name in dicts[0].Keys.ToArray())
            {
                if (!name.IsPositive) continue;
                if (!dicts[0].TryGetValue(name, out var v0) || v0 == null) continue;
                if (!dicts[1].TryGetValue(name, out var v1) || v1 == null) continue;
                var elementType = v0.GetType().GetElementType();
                if (elementType == null || elementType != v1.GetType().GetElementType()) continue;
                var idx = new[] { dicts[0].GetOrDefault(-name) as int[], dicts[1].GetOrDefault(-name) as int[] };
                var normalize = name == PolyMesh.Property.Normals;

                var extra = new List<object>();
                var indices = new int[tris.Count * 3];
                var baseLength = v0.Length + v1.Length;
                for (var i = 0; i < tris.Count; i++)
                {
                    var t = tris[i];
                    var parent = t.Parent;
                    var mi = k.TriMesh[parent];
                    var values = mi == 0 ? v0 : v1;
                    var offset = mi == 0 ? 0 : v0.Length;
                    int SlotValueIndex(int slot) => idx[mi] != null ? idx[mi]![slot] : slot;

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
                            extra.Add(BaryValue(values,
                                SlotValueIndex(k.C0[parent]), SlotValueIndex(k.C1[parent]), SlotValueIndex(k.C2[parent]),
                                w, normalize));
                            at = baseLength + extra.Count - 1;
                        }
                        indices[i * 3 + c] = at;
                    }
                }

                var all = Array.CreateInstance(elementType, baseLength + extra.Count);
                Array.Copy(v0, 0, all, 0, v0.Length);
                Array.Copy(v1, 0, all, v0.Length, v1.Length);
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
