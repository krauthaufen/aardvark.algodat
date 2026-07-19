using Aardvark.Base;
using NUnit.Framework;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// Replays hard boolean cases generated with the Manifold library
    /// (tools/csg-gen-manifold-cases.py) — coaxial/tangent cylinders, near-
    /// tangent and warped spheres, twisted extrusions, tori, gyroid level sets,
    /// sponge-like solids, epsilon-sliver cubes — and compares our results
    /// against Manifold's recorded ground truth.
    /// </summary>
    [TestFixture]
    public class CsgManifoldCases
    {
        private sealed record CaseMesh(double[] Vertices, int[] Triangles);
        private sealed record Case(
            string Name, CaseMesh A, CaseMesh B,
            double UnionVolume, double InterVolume, double DiffVolume,
            double UnionArea, double InterArea, double DiffArea);
        private sealed record CaseFile(Case[] Cases);

        private static PolyMesh ToPolyMesh(CaseMesh m)
        {
            var positions = new V3d[m.Vertices.Length / 3];
            for (var i = 0; i < positions.Length; i++)
                positions[i] = new V3d(m.Vertices[i * 3], m.Vertices[i * 3 + 1], m.Vertices[i * 3 + 2]);
            var fia = new int[m.Triangles.Length / 3 + 1];
            for (var i = 1; i < fia.Length; i++) fia[i] = i * 3;
            return new PolyMesh
            {
                PositionArray = positions,
                FirstIndexArray = fia,
                VertexIndexArray = m.Triangles,
            };
        }

        private static Case[] Load()
        {
            var path = Path.Combine(TestContext.CurrentContext.TestDirectory, "PolyMesh", "manifold-cases.json.gz");
            using var stream = new GZipStream(File.OpenRead(path), CompressionMode.Decompress);
            var file = JsonSerializer.Deserialize<CaseFile>(stream,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;
            return file.Cases;
        }

        [Test]
        public void ReplayManifoldCases()
        {
            var cases = Load();
            Assert.That(cases.Length, Is.GreaterThan(20));
            var failures = 0;
            foreach (var c in cases)
            {
                try
                {
                    var a = ToPolyMesh(c.A);
                    var b = ToPolyMesh(c.B);
                    var arrangement = CsgArrangement.Arrange(a, b);
                    var union = arrangement.Union();
                    var inter = arrangement.Intersection();
                    var diff = arrangement.Difference();

                    var scale = c.UnionVolume.Abs() + c.InterVolume.Abs() + 1e-300;
                    var volTol = 1e-8 * scale;
                    Assert.That(union.Sum(CsgM0Tests.Volume), Is.EqualTo(c.UnionVolume).Within(volTol), $"{c.Name}: union volume");
                    Assert.That(inter.Sum(CsgM0Tests.Volume), Is.EqualTo(c.InterVolume).Within(volTol), $"{c.Name}: inter volume");
                    Assert.That(diff.Sum(CsgM0Tests.Volume), Is.EqualTo(c.DiffVolume).Within(volTol), $"{c.Name}: diff volume");

                    // area comparison only without coincident-surface contact:
                    // measure-zero sheet conventions differ legitimately
                    if (!arrangement.HasCoincidentContact)
                    {
                        Assert.That(union.Sum(Area), Is.EqualTo(c.UnionArea).Within(1e-6 * (1 + c.UnionArea)), $"{c.Name}: union area");
                        Assert.That(inter.Sum(Area), Is.EqualTo(c.InterArea).Within(1e-6 * (1 + c.InterArea)), $"{c.Name}: inter area");
                        Assert.That(diff.Sum(Area), Is.EqualTo(c.DiffArea).Within(1e-6 * (1 + c.DiffArea)), $"{c.Name}: diff area");
                    }
                }
                catch (Exception e) when (e is not AssertionException)
                {
                    failures++;
                    TestContext.Out.WriteLine($"{c.Name}: EXCEPTION {e.Message}");
                }
            }
            Assert.That(failures, Is.EqualTo(0), "cases threw exceptions (see output)");
        }

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
    }
}
