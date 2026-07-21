using System;
using System.Collections.Generic;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// The kernel's SoA working representation: flat vertex/triangle/plane
    /// arrays for both input meshes, with provenance for attribute back-mapping.
    /// Input vertices keep their identity (kernel vertex id = input vertex id +
    /// per-mesh offset); cut vertices are appended with a higher tolerance
    /// generation as the pipeline proceeds.
    /// </summary>
    internal sealed class Kernel
    {
        public Eps Eps;

        // vertices
        public readonly List<V3d> Positions = new();
        /// <summary>per-vertex tolerance factor: 1 for input vertices, conditioning-derived for cuts</summary>
        public readonly List<double> TolFactor = new();

        // --- exact geometry (parallel to Positions) --------------------------
        // Every vertex additionally carries an exact BigInteger position: input
        // vertices are the exact scaled integer of their double coordinates; cut
        // vertices are the exact intersection of the segment and plane that made
        // them. Predicates evaluated on these are mutually consistent, which is
        // what removes float-inconsistency non-manifolds. Populated after
        // UpdateSceneScale via BuildExactInputs, then kept in lock-step at every
        // vertex-append site.
        public readonly List<ExactPoint> Exact = new();
        /// <summary>Scene power-of-two scale: multiplying any input coord by 2^Shift yields an exact integer.</summary>
        public int Shift;
        private readonly List<ExactPlane?> m_exactPlanes = new();

        /// <summary>Exact form of canonical plane <paramref name="id"/> (cached, built on demand).</summary>
        public ExactPlane ExactPlaneOf(int id)
        {
            while (m_exactPlanes.Count <= id) m_exactPlanes.Add(null);
            var p = m_exactPlanes[id];
            if (p == null) { p = ExactPredicates.PlaneFrom(Planes[id], Shift); m_exactPlanes[id] = p; }
            return p.Value;
        }

        /// <summary>
        /// Compute the scene scale and the exact positions of all input vertices.
        /// Call once after all meshes are ingested and UpdateSceneScale has run,
        /// before any cut vertices are constructed.
        /// </summary>
        public void BuildExactInputs()
        {
            int shift = 0;
            for (var i = 0; i < Positions.Count; i++)
            {
                var p = Positions[i];
                shift = Math.Max(shift, ExactPredicates.ShiftFor(stackalloc[] { p.X, p.Y, p.Z }));
            }
            Shift = shift;
            Exact.Clear();
            Exact.Capacity = Positions.Count;
            for (var i = 0; i < Positions.Count; i++)
                Exact.Add(ExactPredicates.ToPoint(Positions[i], shift));
        }

        // triangles (SoA, parallel lists)
        public readonly List<int> T0 = new(), T1 = new(), T2 = new();
        public readonly List<int> TriPlane = new();
        public readonly List<byte> TriMesh = new();  // source mesh: 0 = A, 1 = B
        public readonly List<int> TriFace = new();   // source face index in that mesh
        public readonly List<int> C0 = new(), C1 = new(), C2 = new(); // source face-vertex slot per corner, -1 = none

        // planes (one per source polygon in M0; canonicalized across meshes in M1)
        public readonly List<Plane3d> Planes = new();

        // per input mesh
        public readonly List<int> VertexOffset = new();
        public readonly List<int> VertexCount = new();
        public readonly List<Box3d> Bounds = new();
        /// <summary>source mesh per vertex; -1 for derived (cut) vertices</summary>
        public readonly List<int> VertexMesh = new();

        public int MeshCount => VertexOffset.Count;

        public Kernel(Eps eps) => Eps = eps;

        /// <summary>Sets the scene reference scale on the tolerance model (call after all inputs are ingested).</summary>
        public void UpdateSceneScale()
        {
            var maxMag = 0.0;
            for (var i = 0; i < Positions.Count; i++) maxMag = maxMag.Max(Positions[i].NormMax);
            Eps = Eps.WithScene(maxMag);
        }

        /// <summary>
        /// Debug check (CSG_EXACT_CHECK): the exact list stays in lock-step with
        /// Positions and each exact position agrees with the double position to
        /// within rounding. Returns the max |exact - double| deviation seen.
        /// </summary>
        public double VerifyExact()
        {
            if (Exact.Count != Positions.Count)
                throw new InvalidOperationException($"exact desync: Exact={Exact.Count} Positions={Positions.Count}");
            var maxDev = 0.0;
            for (var i = 0; i < Positions.Count; i++)
            {
                var d = (ExactPredicates.ToV3d(Exact[i], Shift) - Positions[i]).NormMax;
                if (d > maxDev) maxDev = d;
            }
            return maxDev;
        }

        public int TriangleCount => T0.Count;

        /// <summary>Source mesh of a kernel vertex, or -1 for derived vertices.</summary>
        public int VertexSourceMesh(int vi) => vi < VertexMesh.Count ? VertexMesh[vi] : -1;

        /// <summary>
        /// Ingests one input mesh: verifies the watertight-manifold contract,
        /// computes one Newell plane per face (checking planarity against it),
        /// triangulates polygonal faces by ear clipping, and appends everything
        /// to the kernel arrays.
        /// </summary>
        public void Ingest(PolyMesh mesh, int meshIndex, bool verify = true)
        {
            var fia = mesh.FirstIndexArray ?? throw new CsgInputException("mesh has no FirstIndexArray");
            var via = mesh.VertexIndexArray ?? throw new CsgInputException("mesh has no VertexIndexArray");
            var pos = mesh.PositionArray ?? throw new CsgInputException("mesh has no PositionArray");
            var faceCount = fia.Length - 1;

            if (verify)
            {
                var violation = ManifoldChecks.FindManifoldViolation(fia, via, pos.Length);
                if (violation != null)
                    throw new CsgInputException($"input mesh {meshIndex} is not a closed manifold: {violation}");
            }

            if (meshIndex != MeshCount)
                throw new ArgumentException($"meshes must be ingested in order (got index {meshIndex}, expected {MeshCount})");
            if (meshIndex > 255)
                throw new NotSupportedException("at most 256 solids per arrangement");
            var vertexOffset = Positions.Count;
            VertexOffset.Add(vertexOffset);
            VertexCount.Add(pos.Length);
            var bounds = Box3d.Invalid;
            var maxMag = Eps.Scene;
            for (var i = 0; i < pos.Length; i++)
            {
                Positions.Add(pos[i]);
                TolFactor.Add(1.0);
                VertexMesh.Add(meshIndex);
                bounds.ExtendBy(pos[i]);
                maxMag = maxMag.Max(pos[i].NormMax);
            }
            Bounds.Add(bounds);
            // the scene scale must be known before any plane test (planarity
            // below), or tolerances collapse for faces passing near the origin
            Eps = Eps.WithScene(maxMag);

            var polygon = new List<V3d>();
            var polygon2d = new List<V2d>();
            var earTris = new List<(int I0, int I1, int I2)>();

            for (var fi = 0; fi < faceCount; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                var fvc = end - start;

                polygon.Clear();
                for (var i = start; i < end; i++) polygon.Add(pos[via[i]]);

                var plane = Triangulator.NewellPlane(CollectionsMarshalAsSpan(polygon));
                if (plane.Normal == V3d.Zero)
                    throw new CsgInputException($"mesh {meshIndex} face {fi} is degenerate (zero Newell normal)");
                for (var i = 0; i < fvc; i++)
                {
                    if (Eps.HeightSign(plane, polygon[i]) != Sign3.On)
                        throw new CsgInputException(
                            $"mesh {meshIndex} face {fi} is not planar within tolerance " +
                            $"(vertex {via[start + i]} off its face plane)");
                }
                var planeIndex = Planes.Count;
                Planes.Add(plane);

                if (fvc == 3)
                {
                    AddTriangle(
                        vertexOffset + via[start], vertexOffset + via[start + 1], vertexOffset + via[start + 2],
                        planeIndex, meshIndex, fi, start, start + 1, start + 2);
                }
                else
                {
                    polygon2d.Clear();
                    for (var i = 0; i < fvc; i++)
                        polygon2d.Add(Triangulator.ProjectDominant(plane.Normal, polygon[i]));
                    earTris.Clear();
                    if (!Triangulator.EarClip(CollectionsMarshalAsSpan(polygon2d), Eps, earTris))
                        throw new CsgInputException(
                            $"mesh {meshIndex} face {fi} could not be triangulated (self-intersecting?)");
                    foreach (var (i0, i1, i2) in earTris)
                    {
                        AddTriangle(
                            vertexOffset + via[start + i0], vertexOffset + via[start + i1], vertexOffset + via[start + i2],
                            planeIndex, meshIndex, fi, start + i0, start + i1, start + i2);
                    }
                }
            }
        }

        /// <summary>
        /// Appends an already verified/triangulated solid (no verification, no
        /// ear clipping, planes copied — ground truth preserved across chains).
        /// </summary>
        public void IngestPrepared(CsgMesh solid, int meshIndex, int maxThreads = 1)
        {
            if (meshIndex != MeshCount)
                throw new ArgumentException($"meshes must be ingested in order (got index {meshIndex}, expected {MeshCount})");
            if (meshIndex > 255)
                throw new NotSupportedException("at most 256 solids per arrangement");
            var pos = solid.Source.PositionArray;
            var vertexOffset = Positions.Count;
            VertexOffset.Add(vertexOffset);
            VertexCount.Add(pos.Length);
            // pre-size to avoid List growth churn on large solids
            var triCount = solid.T0.Length;
            if (Positions.Capacity < Positions.Count + pos.Length + pos.Length / 8)
                Positions.Capacity = Positions.Count + pos.Length + pos.Length / 8;
            if (TolFactor.Capacity < Positions.Capacity) TolFactor.Capacity = Positions.Capacity;
            if (VertexMesh.Capacity < Positions.Capacity) VertexMesh.Capacity = Positions.Capacity;
            if (T0.Capacity < T0.Count + triCount)
            {
                var cap = T0.Count + triCount;
                T0.Capacity = cap; T1.Capacity = cap; T2.Capacity = cap;
                TriPlane.Capacity = cap; TriMesh.Capacity = cap; TriFace.Capacity = cap;
                C0.Capacity = cap; C1.Capacity = cap; C2.Capacity = cap;
            }
            // vertices: bulk span copies instead of per-element list appends
            var vOld = Positions.Count;
            var vNew = vOld + pos.Length;
            System.Runtime.InteropServices.CollectionsMarshal.SetCount(Positions, vNew);
            System.Runtime.InteropServices.CollectionsMarshal.SetCount(TolFactor, vNew);
            System.Runtime.InteropServices.CollectionsMarshal.SetCount(VertexMesh, vNew);
            var posSpan = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(Positions).Slice(vOld);
            System.Runtime.InteropServices.CollectionsMarshal.AsSpan(TolFactor).Slice(vOld).Fill(1.0);
            System.Runtime.InteropServices.CollectionsMarshal.AsSpan(VertexMesh).Slice(vOld).Fill(meshIndex);
            if (solid.HasTrafo)
            {
                var bounds = Box3d.Invalid;
                var maxMag = 0.0;
                for (var i = 0; i < pos.Length; i++)
                {
                    var p = solid.Trafo.Forward.TransformPos(pos[i]);
                    posSpan[i] = p;
                    bounds.ExtendBy(p);
                    maxMag = maxMag.Max(p.NormMax);
                }
                Bounds.Add(bounds);
                Eps = Eps.WithScene(Eps.Scene.Max(maxMag));
            }
            else
            {
                pos.AsSpan().CopyTo(posSpan);
                Bounds.Add(solid.Bounds3d);
                Eps = Eps.WithScene(Eps.Scene.Max(solid.MeshMag));
            }

            var planeOffset = Planes.Count;
            foreach (var p in solid.Planes)
                Planes.Add(solid.HasTrafo ? solid.TransformPlane(p) : p);

            // triangles: bulk-resize, then a parallel offset-add fill
            var triCount2 = solid.T0.Length;
            var tOld = T0.Count;
            var tNew = tOld + triCount2;
            foreach (var list in new[] { T0, T1, T2, TriPlane, TriFace, C0, C1, C2 })
                System.Runtime.InteropServices.CollectionsMarshal.SetCount(list, tNew);
            System.Runtime.InteropServices.CollectionsMarshal.SetCount(TriMesh, tNew);
            System.Runtime.InteropServices.CollectionsMarshal.AsSpan(TriMesh).Slice(tOld).Fill((byte)meshIndex);
            var t0 = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(T0);
            var t1 = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(T1);
            var t2 = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(T2);
            var tp = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(TriPlane);
            var tf = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(TriFace);
            var c0 = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(C0);
            var c1 = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(C1);
            var c2 = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(C2);
            solid.TriFace.AsSpan().CopyTo(tf.Slice(tOld));
            solid.C0.AsSpan().CopyTo(c0.Slice(tOld));
            solid.C1.AsSpan().CopyTo(c1.Slice(tOld));
            solid.C2.AsSpan().CopyTo(c2.Slice(tOld));
            for (var t = 0; t < triCount2; t++)
            {
                t0[tOld + t] = vertexOffset + solid.T0[t];
                t1[tOld + t] = vertexOffset + solid.T1[t];
                t2[tOld + t] = vertexOffset + solid.T2[t];
                tp[tOld + t] = planeOffset + solid.TriPlane[t];
            }
        }

        private void AddTriangle(int v0, int v1, int v2, int plane, int meshIndex, int face, int c0, int c1, int c2)
        {
            T0.Add(v0); T1.Add(v1); T2.Add(v2);
            TriPlane.Add(plane);
            TriMesh.Add((byte)meshIndex);
            TriFace.Add(face);
            C0.Add(c0); C1.Add(c1); C2.Add(c2);
        }

        private static ReadOnlySpan<T> CollectionsMarshalAsSpan<T>(List<T> list)
            => System.Runtime.InteropServices.CollectionsMarshal.AsSpan(list);
    }
}
