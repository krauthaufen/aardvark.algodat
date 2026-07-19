using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// N-ary combinators: one arrangement over N solids instead of pairwise
    /// cascades. Volumes are cross-checked via inclusion–exclusion.
    /// </summary>
    [TestFixture]
    public class CsgNaryTests
    {
        private static PolyMesh Box(Box3d b) => CsgM0Tests.QuadBox(b);
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

        [Test]
        public void ThreeBoxChainUnion()
        {
            var solids = new[]
            {
                Box(Box3d.Unit),
                Box(new Box3d(new V3d(0.5, 0.25, 0.25), new V3d(1.5, 1.25, 1.25))),
                Box(new Box3d(new V3d(1.0, 0.5, 0.5), new V3d(2.0, 1.5, 1.5))),
            };
            var union = Csg.Union(solids);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));

            // inclusion-exclusion
            var vAB = TotalVolume(Csg.Intersection(solids[0], solids[1]));
            var vAC = TotalVolume(Csg.Intersection(solids[0], solids[2]));
            var vBC = TotalVolume(Csg.Intersection(solids[1], solids[2]));
            var vABC = TotalVolume(Csg.Intersection(solids));
            var expected = solids.Sum(Volume) - vAB - vAC - vBC + vABC;
            Assert.That(TotalVolume(union), Is.EqualTo(expected).Within(1e-9));
        }

        [Test]
        public void ThreeBoxIntersection()
        {
            var solids = new[]
            {
                Box(new Box3d(new V3d(0, 0, 0), new V3d(2, 2, 2))),
                Box(new Box3d(new V3d(1, 0.5, 0.25), new V3d(3, 2.5, 2.25))),
                Box(new Box3d(new V3d(0.5, 1, 0.75), new V3d(2.5, 3, 2.75))),
            };
            var inter = Csg.Intersection(solids);
            AssertManifold(inter);
            // overlap: x [1,2], y [1,2], z [0.75,2]
            Assert.That(TotalVolume(inter), Is.EqualTo(1.0 * 1.0 * 1.25).Within(1e-9));
        }

        [Test]
        public void IdenticalTriplesCollapse()
        {
            var solids = new[] { Box(Box3d.Unit), Box(Box3d.Unit), Box(Box3d.Unit) };
            var union = Csg.Union(solids);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(1.0).Within(1e-9));

            var inter = Csg.Intersection(solids);
            AssertManifold(inter);
            Assert.That(TotalVolume(inter), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void StackedTowerOfThree()
        {
            var solids = new[]
            {
                Box(Box3d.Unit),
                Box(new Box3d(new V3d(0, 0, 1), new V3d(1, 1, 2))),
                Box(new Box3d(new V3d(0, 0, 2), new V3d(1, 1, 3))),
            };
            var union = Csg.Union(solids);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(3.0).Within(1e-9));
        }

        [Test]
        public void BoxMinusThreeHoles()
        {
            var a = Box(new Box3d(new V3d(0, 0, 0), new V3d(4, 2, 1)));
            var holes = new[]
            {
                Box(new Box3d(new V3d(0.5, 0.5, -0.5), new V3d(1.0, 1.5, 1.5))),
                Box(new Box3d(new V3d(1.75, 0.5, -0.5), new V3d(2.25, 1.5, 1.5))),
                Box(new Box3d(new V3d(3.0, 0.5, -0.5), new V3d(3.5, 1.5, 1.5))),
            };
            var diff = Csg.Difference(a, holes);
            AssertManifold(diff);
            Assert.That(diff.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(diff), Is.EqualTo(8.0 - 3 * (0.5 * 1.0 * 1.0)).Within(1e-9));
        }

        [Test]
        public void OverlappingHolesSubtractOnce()
        {
            var a = Box(new Box3d(new V3d(0, 0, 0), new V3d(2, 2, 1)));
            var holes = new[]
            {
                Box(new Box3d(new V3d(0.5, 0.5, -0.5), new V3d(1.25, 1.5, 1.5))),
                Box(new Box3d(new V3d(0.75, 0.5, -0.5), new V3d(1.5, 1.5, 1.5))), // overlaps the first
            };
            var diff = Csg.Difference(a, holes);
            AssertManifold(diff);
            // union of holes inside a: x [0.5,1.5] x y [0.5,1.5] x z [0,1]
            Assert.That(TotalVolume(diff), Is.EqualTo(4.0 - 1.0).Within(1e-9));
        }

        [Test]
        public void FuzzNaryInclusionExclusion()
        {
            var rnd = new RandomSystem(2026);
            for (var i = 0; i < 25; i++)
            {
                var solids = new PolyMesh[3].SetByIndex(_ =>
                {
                    var t = rnd.UniformV3d() * 1.2 - new V3d(0.6);
                    var rot = Trafo3d.RotationEuler(
                        rnd.UniformDouble() * Constant.PiTimesTwo,
                        rnd.UniformDouble() * Constant.PiTimesTwo,
                        rnd.UniformDouble() * Constant.PiTimesTwo);
                    return Box(Box3d.Unit).Transformed(rot * Trafo3d.Translation(t));
                });
                var union = Csg.Union(solids);
                var inter3 = Csg.Intersection(solids);
                AssertManifold(union);
                AssertManifold(inter3);

                var expected = solids.Sum(Volume)
                    - TotalVolume(Csg.Intersection(solids[0], solids[1]))
                    - TotalVolume(Csg.Intersection(solids[0], solids[2]))
                    - TotalVolume(Csg.Intersection(solids[1], solids[2]))
                    + TotalVolume(inter3);
                Assert.That(TotalVolume(union), Is.EqualTo(expected).Within(1e-9 * 3), $"iteration {i}");
            }
        }

        [Test]
        public void NaryFasterThanCascade()
        {
            var solids = new PolyMesh[6].SetByIndex(i =>
                CsgM5Tests.Icosphere(new V3d(i * 0.6, (i % 2) * 0.4, (i % 3) * 0.3), 1.0, 4));

            var sw = Stopwatch.StartNew();
            var nary = Csg.Union(solids);
            var naryMs = sw.ElapsedMilliseconds;
            AssertManifold(nary);

            sw.Restart();
            var cascade = solids[0];
            for (var i = 1; i < solids.Length; i++)
            {
                var r = Csg.Union(cascade, solids[i]);
                Assert.That(r.Length, Is.EqualTo(1));
                cascade = r[0];
            }
            var cascadeMs = sw.ElapsedMilliseconds;

            Assert.That(TotalVolume(nary), Is.EqualTo(Volume(cascade)).Within(1e-9 * solids.Length));
            Console.WriteLine($"n-ary union of 6x5120 tris: {naryMs} ms, pairwise cascade: {cascadeMs} ms");
        }
    }
}
