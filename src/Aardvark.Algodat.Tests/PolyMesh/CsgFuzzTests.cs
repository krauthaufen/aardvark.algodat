using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// Property-based fuzz: random poses (including translations and rotations
    /// snapped to provoke touching/coplanar configurations, at random scales
    /// and offsets) must always yield manifold output satisfying the boolean
    /// volume identities. Deterministic seeds — failures are reproducible.
    /// </summary>
    [TestFixture]
    public class CsgFuzzTests
    {
        private static double Volume(PolyMesh m) => CsgM0Tests.Volume(m);
        private static double TotalVolume(PolyMesh[] meshes) => meshes.Sum(Volume);

        private static void AssertManifold(PolyMesh[] meshes, string context)
        {
            foreach (var m in meshes)
            {
                var violation = ManifoldChecks.FindManifoldViolation(
                    m.FirstIndexArray, m.VertexIndexArray, m.PositionArray.Length);
                Assert.That(violation, Is.Null, context);
            }
        }

        private static Trafo3d RandomPose(RandomSystem rnd, bool snap)
        {
            V3d t;
            Trafo3d rot;
            if (snap)
            {
                // axis-aligned rotation + translation snapped to quarters:
                // provokes coplanar faces, shared edges/vertices, exact touching
                t = new V3d(
                    rnd.UniformInt(9) * 0.25 - 1.0,
                    rnd.UniformInt(9) * 0.25 - 1.0,
                    rnd.UniformInt(9) * 0.25 - 1.0);
                rot = Trafo3d.RotationEulerInDegrees(
                    rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90);
            }
            else
            {
                t = rnd.UniformV3d() * 1.6 - new V3d(0.8);
                rot = Trafo3d.RotationEuler(
                    rnd.UniformDouble() * Constant.PiTimesTwo,
                    rnd.UniformDouble() * Constant.PiTimesTwo,
                    rnd.UniformDouble() * Constant.PiTimesTwo);
            }
            return rot * Trafo3d.Translation(t);
        }

        private static void CheckIdentities(PolyMesh a, PolyMesh b, string context)
        {
            var va = Volume(a);
            var vb = Volume(b);
            var union = Csg.Union(a, b);
            var inter = Csg.Intersection(a, b);
            var diffAB = Csg.Difference(a, b);
            var diffBA = Csg.Difference(b, a);
            AssertManifold(union, context);
            AssertManifold(inter, context);
            AssertManifold(diffAB, context);
            AssertManifold(diffBA, context);

            var scale = va.Abs() + vb.Abs();
            var tol = 1e-9 * scale;
            var vu = TotalVolume(union);
            var vi = TotalVolume(inter);
            Assert.That(vu + vi, Is.EqualTo(va + vb).Within(tol), $"{context}: union+inter");
            Assert.That(TotalVolume(diffAB) + vi, Is.EqualTo(va).Within(tol), $"{context}: diffAB+inter");
            Assert.That(TotalVolume(diffBA) + vi, Is.EqualTo(vb).Within(tol), $"{context}: diffBA+inter");
        }

        [Test]
        public void FuzzBoxesGenericPoses()
        {
            var rnd = new RandomSystem(20260719);
            for (var i = 0; i < 100; i++)
            {
                var a = CsgM0Tests.QuadBox(Box3d.Unit);
                var b = CsgM0Tests.QuadBox(Box3d.Unit).Transformed(RandomPose(rnd, snap: false));
                CheckIdentities(a, b, $"generic iteration {i}");
            }
        }

        [Test]
        public void FuzzBoxesSnappedPoses()
        {
            var rnd = new RandomSystem(77);
            for (var i = 0; i < 200; i++)
            {
                var a = CsgM0Tests.QuadBox(Box3d.Unit);
                var b = CsgM0Tests.QuadBox(Box3d.Unit).Transformed(RandomPose(rnd, snap: true));
                CheckIdentities(a, b, $"snapped iteration {i}");
            }
        }

        [Test]
        public void FuzzBoxesAtScalesAndOffsets()
        {
            var rnd = new RandomSystem(4711);
            for (var i = 0; i < 60; i++)
            {
                var scale = Fun.Pow(10.0, rnd.UniformInt(7) - 3); // 1e-3 .. 1e3
                var offset = (rnd.UniformV3d() * 2 - new V3d(1)) * scale * 1e5;
                var frame = Trafo3d.Scale(scale) * Trafo3d.Translation(offset);
                var snap = (i & 1) == 0;
                var a = CsgM0Tests.QuadBox(Box3d.Unit).Transformed(frame);
                var b = CsgM0Tests.QuadBox(Box3d.Unit).Transformed(RandomPose(rnd, snap) * frame);
                CheckIdentities(a, b, $"scaled iteration {i} (scale {scale}, offset {offset})");
            }
        }

        [Test]
        public void FuzzSphereBox()
        {
            var rnd = new RandomSystem(999);
            for (var i = 0; i < 25; i++)
            {
                var a = CsgM0Tests.QuadBox(Box3d.Unit);
                var b = CsgM5Tests.Icosphere(
                    rnd.UniformV3d() * 1.6 - new V3d(0.3),
                    0.3 + rnd.UniformDouble() * 0.5,
                    2);
                CheckIdentities(a, b, $"sphere-box iteration {i}");
            }
        }
    }
}
