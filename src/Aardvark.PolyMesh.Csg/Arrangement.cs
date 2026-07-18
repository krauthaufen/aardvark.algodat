using System;
using System.Collections.Generic;
using System.Linq;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// The arranged form of two input solids: both meshes ingested into the
    /// kernel with all mutual intersections resolved and every fragment
    /// classified against the other solid. The boolean operations are cheap
    /// selections over this shared arrangement.
    /// </summary>
    public sealed class CsgArrangement
    {
        private readonly Kernel m_kernel;
        private readonly PolyMesh[] m_sources;
        private readonly CsgOptions m_options;

        private CsgArrangement(Kernel kernel, PolyMesh[] sources, CsgOptions options)
        {
            m_kernel = kernel;
            m_sources = sources;
            m_options = options;
        }

        public static CsgArrangement Arrange(PolyMesh a, PolyMesh b, CsgOptions? options = null)
        {
            var o = options ?? CsgOptions.Default;
            var kernel = new Kernel(new Eps(o.RelativeEpsilon));
            if (o.Verification == CsgVerification.None)
                throw new NotSupportedException("input verification cannot be disabled in v0");
            kernel.Ingest(a, 0);
            kernel.Ingest(b, 1);

            // M0: only the disjoint case is arranged; the intersecting pipeline is M2
            var boxA = kernel.Bounds[0];
            var boxB = kernel.Bounds[1];
            var tol = kernel.Eps.Relative * Fun.Max(
                boxA.Min.NormMax.Max(boxA.Max.NormMax),
                boxB.Min.NormMax.Max(boxB.Max.NormMax));
            if (boxA.EnlargedBy(tol).Intersects(boxB.EnlargedBy(tol)))
                throw new NotImplementedException(
                    "CSG of meshes with overlapping bounds is not implemented yet (M2); " +
                    "only disjoint solids are supported in M0");

            return new CsgArrangement(kernel, new[] { a, b }, o);
        }

        /// <summary>Fragments of A outside B plus fragments of B outside A.</summary>
        public PolyMesh[] Union() => Emit(_ => true);

        /// <summary>Fragments of A inside B plus fragments of B inside A.</summary>
        public PolyMesh[] Intersection() => Emit(_ => false);

        /// <summary>Fragments of A outside B plus flipped fragments of B inside A.</summary>
        public PolyMesh[] Difference() => Emit(ti => m_kernel.TriMesh[ti] == 0);

        /// <summary>Symmetric difference.</summary>
        public PolyMesh[] Xor() => Emit(_ => true);

        private PolyMesh[] Emit(Func<int, bool> select)
        {
            var tris = new List<int>();
            for (var ti = 0; ti < m_kernel.TriangleCount; ti++)
                if (select(ti)) tris.Add(ti);
            return Emitter.Emit(m_kernel, tris, m_sources, m_options.Verification == CsgVerification.Full);
        }
    }

    /// <summary>
    /// Builds output PolyMeshes from a kernel triangle selection: one PolyMesh
    /// per edge-connected component, with vertex/face/face-vertex attributes
    /// back-mapped from the source meshes.
    /// </summary>
    internal static class Emitter
    {
        public static PolyMesh[] Emit(Kernel k, List<int> tris, PolyMesh[] sources, bool verify)
        {
            if (tris.Count == 0) return Array.Empty<PolyMesh>();

            // components over kernel vertex ids (no compaction needed for this)
            var fia = new int[tris.Count + 1];
            var via = new int[tris.Count * 3];
            for (var i = 0; i < tris.Count; i++)
            {
                var ti = tris[i];
                fia[i + 1] = (i + 1) * 3;
                via[i * 3] = k.T0[ti];
                via[i * 3 + 1] = k.T1[ti];
                via[i * 3 + 2] = k.T2[ti];
            }
            var componentOfFace = new int[tris.Count];
            var componentCount = ManifoldChecks.EdgeConnectedComponents(fia, via, componentOfFace);

            var result = new PolyMesh[componentCount];
            for (var ci = 0; ci < componentCount; ci++)
            {
                var componentTris = new List<int>();
                for (var i = 0; i < tris.Count; i++)
                    if (componentOfFace[i] == ci) componentTris.Add(tris[i]);
                result[ci] = BuildPolyMesh(k, componentTris, sources, verify);
            }
            return result;
        }

        private static PolyMesh BuildPolyMesh(Kernel k, List<int> tris, PolyMesh[] sources, bool verify)
        {
            var triCount = tris.Count;

            // compact vertices
            var localOfKernel = new Dictionary<int, int>();
            var kernelOfLocal = new List<int>();
            var via = new int[triCount * 3];
            var fia = new int[triCount + 1];
            for (var i = 0; i < triCount; i++)
            {
                var ti = tris[i];
                fia[i + 1] = (i + 1) * 3;
                via[i * 3] = Local(k.T0[ti]);
                via[i * 3 + 1] = Local(k.T1[ti]);
                via[i * 3 + 2] = Local(k.T2[ti]);
            }
            int Local(int vi)
            {
                if (localOfKernel.TryGetValue(vi, out var li)) return li;
                li = kernelOfLocal.Count;
                localOfKernel[vi] = li;
                kernelOfLocal.Add(vi);
                return li;
            }

            var positions = new V3d[kernelOfLocal.Count];
            for (var li = 0; li < positions.Length; li++)
                positions[li] = k.Positions[kernelOfLocal[li]];

            var mesh = new PolyMesh
            {
                PositionArray = positions,
                FirstIndexArray = fia,
                VertexIndexArray = via,
            };

            EmitVertexAttributes(k, mesh, kernelOfLocal, sources);
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

        /// <summary>
        /// Channels usable for output: present in both sources with identical
        /// element type, and non-indexed in both (indexed variants are handled
        /// only for face-vertex attributes, where they are the convention).
        /// </summary>
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
            Kernel k, PolyMesh mesh, List<int> kernelOfLocal, PolyMesh[] sources)
        {
            var dicts = new[] { sources[0].VertexAttributes, sources[1].VertexAttributes };
            foreach (var (name, arrays) in CommonChannels(dicts, PolyMesh.Property.Positions))
            {
                var elementType = arrays[0].GetType().GetElementType()!;
                var target = Array.CreateInstance(elementType, kernelOfLocal.Count);
                var ok = true;
                for (var li = 0; li < kernelOfLocal.Count; li++)
                {
                    var vi = kernelOfLocal[li];
                    var mi = k.VertexSourceMesh(vi);
                    if (mi < 0) { ok = false; break; } // derived vertex: needs interpolation (M2)
                    target.SetValue(arrays[mi].GetValue(vi - k.VertexOffset[mi]), li);
                }
                if (ok) mesh.VertexAttributes[name] = target;
            }
        }

        private static void EmitFaceAttributes(
            Kernel k, PolyMesh mesh, List<int> tris, PolyMesh[] sources)
        {
            var dicts = new[] { sources[0].FaceAttributes, sources[1].FaceAttributes };
            foreach (var (name, arrays) in CommonChannels(dicts))
            {
                var elementType = arrays[0].GetType().GetElementType()!;
                var target = Array.CreateInstance(elementType, tris.Count);
                for (var i = 0; i < tris.Count; i++)
                {
                    var ti = tris[i];
                    target.SetValue(arrays[k.TriMesh[ti]].GetValue(k.TriFace[ti]), i);
                }
                mesh.FaceAttributes[name] = target;
            }
        }

        private static void EmitFaceVertexAttributes(
            Kernel k, PolyMesh mesh, List<int> tris, PolyMesh[] sources)
        {
            // face-vertex channels may be indexed (name = values + -name = indices)
            // or per-slot; output is always emitted in indexed form with the two
            // sources' value arrays concatenated.
            var dicts = new[] { sources[0].FaceVertexAttributes, sources[1].FaceVertexAttributes };
            foreach (var name in dicts[0].Keys.ToArray())
            {
                if (!name.IsPositive) continue;
                if (!dicts[0].TryGetValue(name, out var v0) || v0 == null) continue;
                if (!dicts[1].TryGetValue(name, out var v1) || v1 == null) continue;
                var elementType = v0.GetType().GetElementType();
                if (elementType == null || elementType != v1.GetType().GetElementType()) continue;
                var idx0 = dicts[0].GetOrDefault(-name) as int[];
                var idx1 = dicts[1].GetOrDefault(-name) as int[];

                var values = Array.CreateInstance(elementType, v0.Length + v1.Length);
                Array.Copy(v0, 0, values, 0, v0.Length);
                Array.Copy(v1, 0, values, v0.Length, v1.Length);

                var indices = new int[tris.Count * 3];
                var ok = true;
                for (var i = 0; i < tris.Count && ok; i++)
                {
                    var ti = tris[i];
                    var mi = k.TriMesh[ti];
                    var idx = mi == 0 ? idx0 : idx1;
                    var offset = mi == 0 ? 0 : v0.Length;
                    ok &= MapCorner(k.C0[ti], idx, offset, indices, i * 3);
                    ok &= MapCorner(k.C1[ti], idx, offset, indices, i * 3 + 1);
                    ok &= MapCorner(k.C2[ti], idx, offset, indices, i * 3 + 2);
                }
                if (!ok) continue; // derived corner: needs interpolation (M2)

                mesh.FaceVertexAttributes[name] = values;
                mesh.FaceVertexAttributes[-name] = indices;
            }

            static bool MapCorner(int slot, int[]? sourceIndex, int offset, int[] target, int at)
            {
                if (slot < 0) return false;
                target[at] = offset + (sourceIndex != null ? sourceIndex[slot] : slot);
                return true;
            }
        }

        private static void EmitInstanceAttributes(
            Kernel k, PolyMesh mesh, List<int> tris, PolyMesh[] sources)
        {
            // a component that stems from a single input keeps that input's
            // instance attributes; mixed components inherit A's
            var mi = k.TriMesh[tris[0]];
            for (var i = 1; i < tris.Count; i++)
                if (k.TriMesh[tris[i]] != mi) { mi = 0; break; }
            foreach (var name in sources[mi].InstanceAttributes.Keys.ToArray())
                mesh.InstanceAttributes[name] = sources[mi].InstanceAttributes[name];
        }
    }
}
