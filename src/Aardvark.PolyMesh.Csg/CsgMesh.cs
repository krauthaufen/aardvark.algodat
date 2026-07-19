using System;
using System.Linq;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// A verified watertight-manifold solid in kernel form: triangulated SoA
    /// arrays, one ground-truth plane per source polygon, attribute provenance
    /// into the underlying PolyMesh, and a lazily built, cached BVH.
    ///
    /// Fully immutable: every operation returns a new instance, instances are
    /// safe to share across threads, and FromPolyMesh snapshots the input's
    /// structural arrays so later mutation of the source PolyMesh cannot
    /// corrupt the solid.
    ///
    /// Create once with FromPolyMesh (pays verification + triangulation + plane
    /// fitting once) and reuse across boolean operations; results of boolean
    /// operations are CsgMesh again (manifold by construction, planes carried
    /// through — a face that survives several operations keeps its original
    /// plane instead of accumulating refit error). ToPolyMesh() returns the
    /// PolyMesh form for interop.
    /// </summary>
    public sealed class CsgMesh
    {
        /// <summary>Attribute provider; positions are shared with it.</summary>
        internal readonly PolyMesh Source;
        internal readonly int[] T0, T1, T2;      // triangle vertices (into Source.PositionArray)
        internal readonly Plane3d[] Planes;      // ground-truth planes (unit normals)
        internal readonly int[] TriPlane;        // per tri → plane index
        internal readonly int[] TriFace;         // per tri → Source face id
        internal readonly int[] C0, C1, C2;      // per tri corner → Source face-vertex slot
        internal readonly double MeshMag;        // max |coordinate|
        internal readonly Box3d Bounds3d;
        private readonly bool m_spatiallyOrdered; // boolean results: tris grouped by parent face

        /// <summary>lazy affine transformation (identity when HasTrafo is false); applied on materialization</summary>
        internal readonly Trafo3d Trafo;
        internal readonly bool HasTrafo;

        private volatile BvhCache? m_bvh;
        private sealed record BvhCache(CsgBvh Bvh, double Slack);

        internal CsgMesh(
            PolyMesh source, int[] t0, int[] t1, int[] t2,
            Plane3d[] planes, int[] triPlane, int[] triFace,
            int[] c0, int[] c1, int[] c2, bool spatiallyOrdered = false,
            Trafo3d trafo = default, bool hasTrafo = false)
        {
            m_spatiallyOrdered = spatiallyOrdered;
            Trafo = trafo;
            HasTrafo = hasTrafo;
            Source = source;
            T0 = t0; T1 = t1; T2 = t2;
            Planes = planes; TriPlane = triPlane; TriFace = triFace;
            C0 = c0; C1 = c1; C2 = c2;
            var mag = 0.0;
            var bounds = Box3d.Invalid;
            foreach (var p in source.PositionArray)
            {
                mag = mag.Max(p.NormMax);
                bounds.ExtendBy(p);
            }
            MeshMag = mag;
            Bounds3d = bounds;
        }

        public int TriangleCount => T0.Length;

        /// <summary>Bounds (conservative under a pending lazy transformation).</summary>
        public Box3d Bounds => HasTrafo ? Bounds3d.Transformed(Trafo) : Bounds3d;

        /// <summary>
        /// O(1): returns a solid with the transformation composed onto the
        /// pending one; geometry is transformed lazily on materialization.
        /// Affine, orientation-preserving transformations only (non-uniform
        /// scale is fine — planes map exactly via the inverse transpose).
        /// </summary>
        public CsgMesh Transformed(Trafo3d t)
        {
            var f = t.Forward;
            if (f.M30.Abs() > 1e-15 || f.M31.Abs() > 1e-15 || f.M32.Abs() > 1e-15 || (f.M33 - 1).Abs() > 1e-15)
                throw new NotSupportedException("projective transformations are not supported");
            if (Det3(f) <= 0)
                throw new NotSupportedException("mirroring (negative-determinant) transformations are not supported yet");
            var composed = HasTrafo ? Trafo * t : t;
            return new CsgMesh(Source, T0, T1, T2, Planes, TriPlane, TriFace, C0, C1, C2,
                m_spatiallyOrdered, composed, hasTrafo: true);
        }

        /// <summary>The pending transformation applied to the arrays (identity: this).</summary>
        public CsgMesh Materialized()
        {
            if (!HasTrafo) return this;
            var pos = Source.PositionArray.Map(p => Trafo.Forward.TransformPos(p));
            var mesh = ShallowWrap(Source);
            mesh.PositionArray = pos;
            if (mesh.VertexAttributes.GetOrDefault(PolyMesh.Property.Normals) is V3d[] normals)
                mesh.VertexAttributes[PolyMesh.Property.Normals] =
                    normals.Map(n => Trafo.Backward.TransposedTransformDir(n).Normalized);
            var planes = new Plane3d[Planes.Length];
            for (var i = 0; i < planes.Length; i++) planes[i] = TransformPlane(Planes[i]);
            return new CsgMesh(mesh, T0, T1, T2, planes, TriPlane, TriFace, C0, C1, C2, m_spatiallyOrdered);
        }

        internal Plane3d TransformPlane(in Plane3d plane)
        {
            var n = Trafo.Backward.TransposedTransformDir(plane.Normal).Normalized;
            var p0 = Trafo.Forward.TransformPos(plane.Normal * plane.Distance);
            return new Plane3d(n, n.Dot(p0));
        }

        private static double Det3(in M44d m)
            => m.M00 * (m.M11 * m.M22 - m.M12 * m.M21)
             - m.M01 * (m.M10 * m.M22 - m.M12 * m.M20)
             + m.M02 * (m.M10 * m.M21 - m.M11 * m.M20);

        /// <summary>
        /// The solid as a PolyMesh (any pending transformation applied). The
        /// returned instance is a fresh wrapper (mutating its attribute
        /// dictionaries cannot affect this solid); the bulk arrays are shared
        /// and must be treated as read-only.
        /// </summary>
        public PolyMesh ToPolyMesh() => HasTrafo ? Materialized().Source : ShallowWrap(Source);

        private static PolyMesh ShallowWrap(PolyMesh m)
        {
            var result = new PolyMesh
            {
                PositionArray = m.PositionArray,
                FirstIndexArray = m.FirstIndexArray,
                VertexIndexArray = m.VertexIndexArray,
            };
            foreach (var name in m.VertexAttributes.Keys.ToArray())
                if (name != PolyMesh.Property.Positions) result.VertexAttributes[name] = m.VertexAttributes[name];
            foreach (var name in m.FaceAttributes.Keys.ToArray()) result.FaceAttributes[name] = m.FaceAttributes[name];
            foreach (var name in m.FaceVertexAttributes.Keys.ToArray()) result.FaceVertexAttributes[name] = m.FaceVertexAttributes[name];
            foreach (var name in m.EdgeAttributes.Keys.ToArray()) result.EdgeAttributes[name] = m.EdgeAttributes[name];
            foreach (var name in m.InstanceAttributes.Keys.ToArray()) result.InstanceAttributes[name] = m.InstanceAttributes[name];
            return result;
        }

        /// <summary>Signed volume (positive for outward-oriented solids), centroid-relative.</summary>
        public double Volume
        {
            get
            {
                if (HasTrafo) return BaseVolume * Det3(Trafo.Forward);
                return BaseVolume;
            }
        }

        private double BaseVolume
        {
            get
            {
                var pos = Source.PositionArray;
                var centroid = V3d.Zero;
                foreach (var p in pos) centroid += p;
                centroid /= pos.Length.Max(1);
                var sum = 0.0;
                for (var t = 0; t < T0.Length; t++)
                    sum += (pos[T0[t]] - centroid).Dot((pos[T1[t]] - centroid).Cross(pos[T2[t]] - centroid));
                return sum / 6.0;
            }
        }

        public double SurfaceArea
        {
            get
            {
                var self = Materialized();
                var pos = self.Source.PositionArray;
                var sum = 0.0;
                for (var t = 0; t < self.T0.Length; t++)
                    sum += 0.5 * (pos[self.T1[t]] - pos[self.T0[t]]).Cross(pos[self.T2[t]] - pos[self.T0[t]]).Length;
                return sum;
            }
        }

        /// <summary>
        /// Verifies, triangulates and plane-fits a watertight manifold PolyMesh
        /// into reusable kernel form. Throws CsgInputException on contract
        /// violations.
        /// </summary>
        public static CsgMesh FromPolyMesh(PolyMesh mesh, CsgOptions? options = null)
        {
            var o = options ?? CsgOptions.Default;
            var kernel = new Kernel(new Eps(o.RelativeEpsilon));
            kernel.Ingest(mesh, 0);
            // snapshot the structural arrays: later mutation of the input
            // PolyMesh must not be able to invalidate the verified solid
            var snapshot = ShallowWrap(mesh);
            snapshot.PositionArray = mesh.PositionArray.Copy();
            snapshot.FirstIndexArray = mesh.FirstIndexArray.Copy();
            snapshot.VertexIndexArray = mesh.VertexIndexArray.Copy();
            return new CsgMesh(
                snapshot,
                kernel.T0.ToArray(), kernel.T1.ToArray(), kernel.T2.ToArray(),
                kernel.Planes.ToArray(),
                kernel.TriPlane.ToArray(),
                kernel.TriFace.ToArray(),
                kernel.C0.ToArray(), kernel.C1.ToArray(), kernel.C2.ToArray());
        }

        #region operators

        // set-style operators (the C# idiom for set semantics — with these,
        // A | B == (A - B) | (B - A) | (A & B) reads as the set identity it is):
        // '|' union, '&' intersection, '-' difference, '^' symmetric difference.
        // Multi-component results are merged into one CsgMesh; use the Csg /
        // CsgArrangement APIs to get individual components or to pass options.

        public static CsgMesh operator |(CsgMesh a, CsgMesh b)
            => Merge(CsgArrangement.Arrange(a, b).UnionSolids());

        public static CsgMesh operator &(CsgMesh a, CsgMesh b)
            => Merge(CsgArrangement.Arrange(a, b).IntersectionSolids());

        public static CsgMesh operator -(CsgMesh a, CsgMesh b)
            => Merge(CsgArrangement.Arrange(a, b).DifferenceSolids());

        public static CsgMesh operator ^(CsgMesh a, CsgMesh b)
            => Merge(CsgArrangement.Arrange(a, b).XorSolids());

        /// <summary>An empty solid (used for empty operator results).</summary>
        public static readonly CsgMesh Empty = new(
            new PolyMesh
            {
                PositionArray = Array.Empty<V3d>(),
                FirstIndexArray = new[] { 0 },
                VertexIndexArray = Array.Empty<int>(),
            },
            Array.Empty<int>(), Array.Empty<int>(), Array.Empty<int>(),
            Array.Empty<Plane3d>(), Array.Empty<int>(), Array.Empty<int>(),
            Array.Empty<int>(), Array.Empty<int>(), Array.Empty<int>());

        /// <summary>
        /// Concatenates component solids into one CsgMesh (components stay
        /// separate surfaces; attribute channels are concatenated).
        /// </summary>
        public static CsgMesh Merge(CsgMesh[] parts)
        {
            if (parts.Length == 0) return Empty;
            if (parts.Length == 1) return parts[0];
            parts = parts.Map(p => p.Materialized());

            var vertexOffsets = new int[parts.Length];
            var triOffsets = new int[parts.Length];
            var planeOffsets = new int[parts.Length];
            var slotOffsets = new int[parts.Length];
            int vTotal = 0, tTotal = 0, pTotal = 0, sTotal = 0;
            for (var i = 0; i < parts.Length; i++)
            {
                vertexOffsets[i] = vTotal; triOffsets[i] = tTotal; planeOffsets[i] = pTotal; slotOffsets[i] = sTotal;
                vTotal += parts[i].Source.PositionArray.Length;
                tTotal += parts[i].TriangleCount;
                pTotal += parts[i].Planes.Length;
                sTotal += parts[i].Source.VertexIndexArray.Length;
            }

            var t0 = new int[tTotal]; var t1 = new int[tTotal]; var t2 = new int[tTotal];
            var planes = new Plane3d[pTotal];
            var triPlane = new int[tTotal]; var triFace = new int[tTotal];
            var c0 = new int[tTotal]; var c1 = new int[tTotal]; var c2 = new int[tTotal];
            var positions = new V3d[vTotal];
            var fia = new int[tTotal + 1];
            var via = new int[tTotal * 3];

            // NOTE: components are emitted as triangle meshes, so face id == tri id
            for (var i = 0; i < parts.Length; i++)
            {
                var part = parts[i];
                part.Source.PositionArray.CopyTo(positions, vertexOffsets[i]);
                part.Planes.CopyTo(planes, planeOffsets[i]);
                for (var t = 0; t < part.TriangleCount; t++)
                {
                    var ot = triOffsets[i] + t;
                    t0[ot] = vertexOffsets[i] + part.T0[t];
                    t1[ot] = vertexOffsets[i] + part.T1[t];
                    t2[ot] = vertexOffsets[i] + part.T2[t];
                    triPlane[ot] = planeOffsets[i] + part.TriPlane[t];
                    triFace[ot] = ot;
                    c0[ot] = slotOffsets[i] + part.C0[t];
                    c1[ot] = slotOffsets[i] + part.C1[t];
                    c2[ot] = slotOffsets[i] + part.C2[t];
                    fia[ot + 1] = (ot + 1) * 3;
                    via[ot * 3] = t0[ot]; via[ot * 3 + 1] = t1[ot]; via[ot * 3 + 2] = t2[ot];
                }
            }

            var mesh = new PolyMesh
            {
                PositionArray = positions,
                FirstIndexArray = fia,
                VertexIndexArray = via,
            };
            MergeChannels(parts.Map(x => x.Source), mesh, vertexOffsets, triOffsets, slotOffsets);
            return new CsgMesh(mesh, t0, t1, t2, planes, triPlane, triFace, c0, c1, c2);
        }

        private static void MergeChannels(
            PolyMesh[] sources, PolyMesh target, int[] vertexOffsets, int[] faceOffsets, int[] slotOffsets)
        {
            void Concat(
                Func<PolyMesh, SymbolDict<Array>> dictOf, SymbolDict<Array> targetDict,
                int[] offsets, int total, Symbol skip)
            {
                foreach (var name in dictOf(sources[0]).Keys.ToArray())
                {
                    if (!name.IsPositive || name == skip) continue;
                    var arrays = new Array[sources.Length];
                    var indexArrays = new int[sources.Length][];
                    var ok = true;
                    var valueTotal = 0;
                    for (var i = 0; i < sources.Length && ok; i++)
                    {
                        ok = dictOf(sources[i]).TryGetValue(name, out var a) && a != null
                            && a.GetType().GetElementType() == dictOf(sources[0])[name].GetType().GetElementType();
                        if (!ok) break;
                        arrays[i] = a!;
                        indexArrays[i] = dictOf(sources[i]).GetOrDefault(-name) as int[];
                        valueTotal += a!.Length;
                    }
                    if (!ok) continue;
                    var anyIndexed = indexArrays.Any(x => x != null);
                    var values = Array.CreateInstance(arrays[0].GetType().GetElementType()!, valueTotal);
                    var valueOffset = 0;
                    var indices = anyIndexed ? new int[total] : null;
                    for (var i = 0; i < sources.Length; i++)
                    {
                        arrays[i].CopyTo(values, valueOffset);
                        if (indices != null)
                        {
                            var count = i + 1 < sources.Length ? offsets[i + 1] - offsets[i] : total - offsets[i];
                            for (var e = 0; e < count; e++)
                                indices[offsets[i] + e] = valueOffset + (indexArrays[i] != null ? indexArrays[i][e] : e);
                        }
                        valueOffset += arrays[i].Length;
                    }
                    targetDict[name] = values;
                    if (indices != null) targetDict[-name] = indices;
                }
            }

            Concat(m => m.VertexAttributes, target.VertexAttributes, vertexOffsets, target.PositionArray.Length, PolyMesh.Property.Positions);
            Concat(m => m.FaceAttributes, target.FaceAttributes, faceOffsets, target.FirstIndexArray.Length - 1, default);
            Concat(m => m.FaceVertexAttributes, target.FaceVertexAttributes, slotOffsets, target.VertexIndexArray.Length, default);
            foreach (var name in sources[0].InstanceAttributes.Keys.ToArray())
                target.InstanceAttributes[name] = sources[0].InstanceAttributes[name];
        }

        #endregion

        /// <summary>
        /// The cached BVH over triangle boxes, enlarged by at least
        /// requiredSlack (rebuilt if a previous build used a smaller slack).
        /// The generous default margin covers eps-welding position shifts.
        /// </summary>
        internal CsgBvh Bvh(double requiredSlack)
        {
            var cached = m_bvh;
            if (cached != null && cached.Slack >= requiredSlack) return cached.Bvh;
            var slack = requiredSlack.Max(1e-300) * 2; // headroom so nearby eps configurations reuse it
            var pos = Source.PositionArray;
            var boxes = new Box3d[T0.Length];
            if (HasTrafo)
            {
                var tp = pos.Map(p => Trafo.Forward.TransformPos(p));
                for (var t = 0; t < T0.Length; t++)
                    boxes[t] = new Box3d(tp[T0[t]], tp[T1[t]], tp[T2[t]]).EnlargedBy(slack);
            }
            else
            {
                for (var t = 0; t < T0.Length; t++)
                    boxes[t] = new Box3d(pos[T0[t]], pos[T1[t]], pos[T2[t]]).EnlargedBy(slack);
            }
            var built = new BvhCache(new CsgBvh(boxes, presorted: m_spatiallyOrdered), slack);
            m_bvh = built; // benign race: losers rebuild, winners stay consistent
            return built.Bvh;
        }
    }
}
