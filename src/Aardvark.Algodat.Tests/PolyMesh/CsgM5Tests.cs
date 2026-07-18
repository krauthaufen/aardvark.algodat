using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// Stress: dense curved meshes (icospheres) — long intersection curves
    /// across many faces, identical-mesh coplanar handling at scale, and a
    /// perf smoke test.
    /// </summary>
    [TestFixture]
    public class CsgM5Tests
    {
        private static double Volume(PolyMesh m) => CsgM0Tests.Volume(m);
        private static double TotalVolume(PolyMesh[] meshes) => meshes.Sum(Volume);

        private static void AssertManifold(PolyMesh[] meshes)
        {
            foreach (var m in meshes)
            {
                var violation = ManifoldChecks.FindManifoldViolation(
                    m.FirstIndexArray, m.VertexIndexArray, m.PositionArray.Length);
                Assert.That(violation, Is.Null);
            }
        }

        /// <summary>Unit icosphere around center, hand-built (independent of PolyMeshPrimitives).</summary>
        internal static PolyMesh Icosphere(V3d center, double radius, int subdivisions)
        {
            var t = (1.0 + Fun.Sqrt(5.0)) / 2.0;
            var verts = new List<V3d>
            {
                new(-1,  t,  0), new( 1,  t,  0), new(-1, -t,  0), new( 1, -t,  0),
                new( 0, -1,  t), new( 0,  1,  t), new( 0, -1, -t), new( 0,  1, -t),
                new( t,  0, -1), new( t,  0,  1), new(-t,  0, -1), new(-t,  0,  1),
            }.Map(v => v.Normalized).ToList();
            var faces = new List<(int, int, int)>
            {
                (0, 11, 5), (0, 5, 1), (0, 1, 7), (0, 7, 10), (0, 10, 11),
                (1, 5, 9), (5, 11, 4), (11, 10, 2), (10, 7, 6), (7, 1, 8),
                (3, 9, 4), (3, 4, 2), (3, 2, 6), (3, 6, 8), (3, 8, 9),
                (4, 9, 5), (2, 4, 11), (6, 2, 10), (8, 6, 7), (9, 8, 1),
            };

            for (var s = 0; s < subdivisions; s++)
            {
                var midpoint = new Dictionary<(int, int), int>();
                int Mid(int a, int b)
                {
                    var key = a < b ? (a, b) : (b, a);
                    if (midpoint.TryGetValue(key, out var m)) return m;
                    m = verts.Count;
                    verts.Add(((verts[a] + verts[b]) * 0.5).Normalized);
                    midpoint[key] = m;
                    return m;
                }
                var next = new List<(int, int, int)>(faces.Count * 4);
                foreach (var (a, b, c) in faces)
                {
                    var ab = Mid(a, b); var bc = Mid(b, c); var ca = Mid(c, a);
                    next.Add((a, ab, ca)); next.Add((b, bc, ab)); next.Add((c, ca, bc)); next.Add((ab, bc, ca));
                }
                faces = next;
            }

            var fia = new int[faces.Count + 1];
            for (var i = 0; i < faces.Count; i++) fia[i + 1] = (i + 1) * 3;
            var via = new int[faces.Count * 3];
            for (var i = 0; i < faces.Count; i++)
            {
                via[i * 3] = faces[i].Item1;
                via[i * 3 + 1] = faces[i].Item2;
                via[i * 3 + 2] = faces[i].Item3;
            }
            return new PolyMesh
            {
                PositionArray = verts.Map(v => center + radius * v).ToArray(),
                FirstIndexArray = fia,
                VertexIndexArray = via,
            };
        }

        [Test]
        public void SphereSphereBooleans()
        {
            var a = Icosphere(V3d.Zero, 1.0, 3);           // 1280 tris
            var b = Icosphere(new V3d(1, 0, 0), 1.0, 3);

            var union = Csg.Union(a, b);
            var inter = Csg.Intersection(a, b);
            var diff = Csg.Difference(a, b);
            AssertManifold(union); AssertManifold(inter); AssertManifold(diff);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(inter.Length, Is.EqualTo(1));

            var va = Volume(a); var vb = Volume(b);
            var vu = TotalVolume(union); var vi = TotalVolume(inter);
            Assert.That(vu + vi, Is.EqualTo(va + vb).Within(1e-9));
            Assert.That(TotalVolume(diff) + vi, Is.EqualTo(va).Within(1e-9));

            // analytic lens volume for r=1, d=1: V = 2*pi*(2/3 - 5/16)... use
            // the spherical cap formula: V = 2 * pi*h^2*(r - h/3), h = 1/2
            var lens = 2 * (Constant.Pi * 0.25 * (1 - 1.0 / 6));
            Assert.That(vi, Is.EqualTo(lens).Within(0.05 * lens)); // mesh approximation
        }

        [Test]
        public void IdenticalSpheresCollapseToOne()
        {
            var a = Icosphere(V3d.Zero, 1.0, 2);
            var b = Icosphere(V3d.Zero, 1.0, 2);
            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(Volume(a)).Within(1e-9));
            Assert.That(TotalVolume(Csg.Intersection(a, b)), Is.EqualTo(Volume(a)).Within(1e-9));
            Assert.That(Csg.Difference(a, b), Is.Empty);
            Assert.That(Csg.Xor(a, b), Is.Empty);
        }

        [Test]
        public void SphereBoxDifference()
        {
            var a = CsgM0Tests.QuadBox(Box3d.Unit);
            var b = Icosphere(new V3d(0.5, 0.5, 1.0), 0.4, 3); // ball centered on the top face
            var diff = Csg.Difference(a, b);
            AssertManifold(diff);
            var halfBall = 0.5 * (4.0 / 3.0) * Constant.Pi * 0.4 * 0.4 * 0.4;
            Assert.That(TotalVolume(diff), Is.EqualTo(1.0 - halfBall).Within(0.02 * halfBall));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(TotalVolume(union), Is.EqualTo(1.0 + halfBall).Within(0.02 * halfBall));
        }

        [Test]
        public void PerfSmoke()
        {
            var a = Icosphere(V3d.Zero, 1.0, 4);           // 5120 tris
            var b = Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, 4);
            var sw = Stopwatch.StartNew();
            var union = Csg.Union(a, b);
            sw.Stop();
            AssertManifold(union);
            Assert.That(TotalVolume(union), Is.GreaterThan(Volume(a)));
            Console.WriteLine($"union of 2x5120 tris: {sw.ElapsedMilliseconds} ms");
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(30_000), "performance smoke: should finish well under 30s");
        }
    }
}
