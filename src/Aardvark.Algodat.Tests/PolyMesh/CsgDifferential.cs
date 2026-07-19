using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// Differential-testing exporter: generates a deterministic zoo of fuzzed
    /// solid pairs, runs our booleans, and dumps inputs + our volumes/areas as
    /// JSON for comparison against the Manifold library
    /// (tools/csg-diff.py; see tools/README-csg-diff.md).
    /// </summary>
    [TestFixture]
    public class CsgDifferential
    {
        private static double Volume(PolyMesh m) => CsgM0Tests.Volume(m);

        private static double Area(PolyMesh m)
        {
            var fia = m.FirstIndexArray; var via = m.VertexIndexArray; var pos = m.PositionArray;
            var sum = 0.0;
            for (var fi = 0; fi + 1 < fia.Length; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                var p0 = pos[via[start]];
                for (var i = start + 1; i + 1 < end; i++)
                    sum += 0.5 * (pos[via[i]] - p0).Cross(pos[via[i + 1]] - p0).Length;
            }
            return sum;
        }

        // NOTE: uses the kernel's own ear clipping — PolyMesh.TriangulatedCopy
        // mis-triangulates reversed concave caps (folded, double-covered area),
        // which this differential harness itself uncovered.
        private static object ExportMesh(PolyMesh m)
        {
            var fia = m.FirstIndexArray; var via = m.VertexIndexArray; var pos = m.PositionArray;
            var tris = new List<int>();
            var polygon = new List<V3d>();
            var polygon2d = new List<V2d>();
            var ears = new List<(int, int, int)>();
            var eps = new Eps(1e-11, pos.Max(p => p.NormMax));
            for (var fi = 0; fi + 1 < fia.Length; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                if (end - start == 3)
                {
                    tris.Add(via[start]); tris.Add(via[start + 1]); tris.Add(via[start + 2]);
                    continue;
                }
                polygon.Clear(); polygon2d.Clear(); ears.Clear();
                for (var i = start; i < end; i++) polygon.Add(pos[via[i]]);
                var plane = Triangulator.NewellPlane(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(polygon));
                for (var i = 0; i < polygon.Count; i++)
                    polygon2d.Add(Triangulator.ProjectDominant(plane.Normal, polygon[i]));
                if (!Triangulator.EarClip(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(polygon2d), eps, ears))
                    throw new InvalidOperationException($"export triangulation failed for face {fi}");
                foreach (var (i0, i1, i2) in ears)
                {
                    tris.Add(via[start + i0]); tris.Add(via[start + i1]); tris.Add(via[start + i2]);
                }
            }
            return new
            {
                vertices = pos.SelectMany(p => new[] { p.X, p.Y, p.Z }).ToArray(),
                triangles = tris.ToArray(),
            };
        }

        /// <summary>True if the pair has coincident (coplanar) surface regions — area
        /// comparisons against Manifold are skipped there (measure-zero contact
        /// conventions differ legitimately).</summary>
        private static bool HasCoplanarContact(PolyMesh a, PolyMesh b)
        {
            var kernel = new Kernel(new Eps(1e-11));
            kernel.Ingest(a, 0);
            kernel.Ingest(b, 1);
            var pipeline = new Pipeline(kernel);
            pipeline.Run();
            return pipeline.Labels.Any(l => l == FragLabel.OnSame || l == FragLabel.OnOpposite);
        }

        [Test]
        [Explicit]
        public void Export()
        {
            var cases = new List<object>();

            void AddCase(string name, PolyMesh a, PolyMesh b)
            {
                object result;
                try
                {
                    var union = Csg.Union(a, b);
                    var inter = Csg.Intersection(a, b);
                    var diff = Csg.Difference(a, b);
                    result = new
                    {
                        name,
                        a = ExportMesh(a),
                        b = ExportMesh(b),
                        coplanarContact = HasCoplanarContact(a, b),
                        unionVolume = union.Sum(Volume),
                        interVolume = inter.Sum(Volume),
                        diffVolume = diff.Sum(Volume),
                        unionArea = union.Sum(Area),
                        interArea = inter.Sum(Area),
                        diffArea = diff.Sum(Area),
                    };
                }
                catch (Exception e)
                {
                    result = new { name, a = ExportMesh(a), b = ExportMesh(b), failed = e.Message };
                }
                cases.Add(result);
            }

            var rnd = new RandomSystem(20260719);

            for (var i = 0; i < 150; i++)
            {
                var t = rnd.UniformV3d() * 2.4 - new V3d(1.2);
                var rot = Trafo3d.RotationEuler(
                    rnd.UniformDouble() * Constant.PiTimesTwo,
                    rnd.UniformDouble() * Constant.PiTimesTwo,
                    rnd.UniformDouble() * Constant.PiTimesTwo);
                AddCase($"box-generic-{i}",
                    CsgM0Tests.QuadBox(Box3d.Unit),
                    CsgM0Tests.QuadBox(Box3d.Unit).Transformed(rot * Trafo3d.Translation(t)));
            }

            for (var i = 0; i < 150; i++)
            {
                var t = new V3d(
                    rnd.UniformInt(17) * 0.25 - 2, rnd.UniformInt(17) * 0.25 - 2, rnd.UniformInt(17) * 0.25 - 2);
                var rot = Trafo3d.RotationEulerInDegrees(
                    rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90);
                var size = new V3d(1 + rnd.UniformInt(3) * 0.5, 1, 1);
                AddCase($"box-snapped-{i}",
                    CsgM0Tests.QuadBox(Box3d.Unit),
                    CsgM0Tests.QuadBox(new Box3d(V3d.Zero, size)).Transformed(rot * Trafo3d.Translation(t)));
            }

            for (var i = 0; i < 40; i++)
            {
                var c = rnd.UniformV3d() * 2 - new V3d(0.5);
                AddCase($"spheres-{i}",
                    CsgM5Tests.Icosphere(V3d.Zero, 1.0, 3),
                    CsgM5Tests.Icosphere(c, 0.4 + rnd.UniformDouble(), 3));
            }

            for (var i = 0; i < 60; i++)
            {
                var snap = (i & 1) == 0;
                var t = snap
                    ? new V3d(rnd.UniformInt(13) * 0.5 - 2, rnd.UniformInt(13) * 0.5 - 2, rnd.UniformInt(9) * 0.5 - 1)
                    : rnd.UniformV3d() * 4 - new V3d(1.5);
                var rot = snap
                    ? Trafo3d.RotationZInDegrees(rnd.UniformInt(4) * 90)
                    : Trafo3d.RotationEuler(
                        rnd.UniformDouble() * Constant.PiTimesTwo,
                        rnd.UniformDouble() * Constant.PiTimesTwo,
                        rnd.UniformDouble() * Constant.PiTimesTwo);
                AddCase($"lprism-{i}",
                    CsgM6Tests.LPrism(),
                    CsgM6Tests.LPrism().Transformed(rot * Trafo3d.Translation(t)));
            }

            var path = Environment.GetEnvironmentVariable("CSG_DIFF_OUT") ?? "/tmp/csgdiff-cases.json";
            File.WriteAllText(path, JsonSerializer.Serialize(new { cases }));
            Console.WriteLine($"exported {cases.Count} cases to {path}");
        }
    }
}
