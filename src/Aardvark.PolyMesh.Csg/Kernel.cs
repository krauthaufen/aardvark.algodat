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
        public readonly List<byte> Generation = new();

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

        public int TriangleCount => T0.Count;

        /// <summary>Source mesh of a kernel vertex, or -1 for derived vertices.</summary>
        public int VertexSourceMesh(int vi) => vi < VertexMesh.Count ? VertexMesh[vi] : -1;

        /// <summary>
        /// Ingests one input mesh: verifies the watertight-manifold contract,
        /// computes one Newell plane per face (checking planarity against it),
        /// triangulates polygonal faces by ear clipping, and appends everything
        /// to the kernel arrays.
        /// </summary>
        public void Ingest(PolyMesh mesh, int meshIndex)
        {
            var fia = mesh.FirstIndexArray ?? throw new CsgInputException("mesh has no FirstIndexArray");
            var via = mesh.VertexIndexArray ?? throw new CsgInputException("mesh has no VertexIndexArray");
            var pos = mesh.PositionArray ?? throw new CsgInputException("mesh has no PositionArray");
            var faceCount = fia.Length - 1;

            var violation = ManifoldChecks.FindManifoldViolation(fia, via, pos.Length);
            if (violation != null)
                throw new CsgInputException($"input mesh {meshIndex} is not a closed manifold: {violation}");

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
                Generation.Add(0);
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
        public void IngestPrepared(CsgMesh solid, int meshIndex)
        {
            if (meshIndex != MeshCount)
                throw new ArgumentException($"meshes must be ingested in order (got index {meshIndex}, expected {MeshCount})");
            if (meshIndex > 255)
                throw new NotSupportedException("at most 256 solids per arrangement");
            var pos = solid.Source.PositionArray;
            var vertexOffset = Positions.Count;
            VertexOffset.Add(vertexOffset);
            VertexCount.Add(pos.Length);
            if (solid.HasTrafo)
            {
                // materialize the lazy transformation while copying
                var bounds = Box3d.Invalid;
                var maxMag = 0.0;
                for (var i = 0; i < pos.Length; i++)
                {
                    var p = solid.Trafo.Forward.TransformPos(pos[i]);
                    Positions.Add(p);
                    Generation.Add(0);
                    VertexMesh.Add(meshIndex);
                    bounds.ExtendBy(p);
                    maxMag = maxMag.Max(p.NormMax);
                }
                Bounds.Add(bounds);
                Eps = Eps.WithScene(Eps.Scene.Max(maxMag));
            }
            else
            {
                for (var i = 0; i < pos.Length; i++)
                {
                    Positions.Add(pos[i]);
                    Generation.Add(0);
                    VertexMesh.Add(meshIndex);
                }
                Bounds.Add(solid.Bounds3d);
                Eps = Eps.WithScene(Eps.Scene.Max(solid.MeshMag));
            }

            var planeOffset = Planes.Count;
            foreach (var p in solid.Planes)
                Planes.Add(solid.HasTrafo ? solid.TransformPlane(p) : p);
            for (var t = 0; t < solid.T0.Length; t++)
            {
                AddTriangle(
                    vertexOffset + solid.T0[t], vertexOffset + solid.T1[t], vertexOffset + solid.T2[t],
                    planeOffset + solid.TriPlane[t], meshIndex, solid.TriFace[t],
                    solid.C0[t], solid.C1[t], solid.C2[t]);
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
