using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// On-case matrix: solids in tangent/touching configurations, where eps
    /// classification must produce On states and the output must still be
    /// manifold (touching solids emit as separate components).
    /// </summary>
    [TestFixture]
    public class CsgM3Tests
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

        /// <summary>Box rotated 45° about X so its lowest edge lies exactly in the z=1 plane, running from (0.2,0.5,1) to (0.8,0.5,1).</summary>
        private static PolyMesh EdgeRestingBox()
        {
            var box = Box(new Box3d(new V3d(-0.3, -0.5, -0.5), new V3d(0.3, 0.5, 0.5)));
            return box.Transformed(
                Trafo3d.RotationX(Constant.PiQuarter)
                * Trafo3d.Translation(0.5, 0.5, 1.0 + Constant.Sqrt2Half));
        }

        [Test]
        public void EdgeRestingOnFaceUnionSplitsIntoTwoSolids()
        {
            var a = Box(Box3d.Unit);
            var b = EdgeRestingBox();

            // sanity: the lowest edge of b must lie exactly on z=1
            var minZ = b.PositionArray.Min(p => p.Z);
            Assert.That(minZ, Is.EqualTo(1.0).Within(1e-12));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(2));
            Assert.That(TotalVolume(union), Is.EqualTo(1.6).Within(1e-9)); // 1 + 0.6*1*1

            Assert.That(Csg.Intersection(a, b), Is.Empty);

            var diff = Csg.Difference(a, b);
            AssertManifold(diff);
            Assert.That(TotalVolume(diff), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void CornerTouchingFaceKeepsSolidsSeparate()
        {
            var a = Box(Box3d.Unit);
            // rotate a box so one corner points down, place that corner on the top face of a
            var cornerDown = Trafo3d.RotationEuler(Constant.PiQuarter, Fun.Atan(Constant.Sqrt2Half), 0);
            var b0 = Box(new Box3d(new V3d(-0.5), new V3d(0.5))).Transformed(cornerDown);
            var minZ = b0.PositionArray.Min(p => p.Z);
            var b = b0.Transformed(Trafo3d.Translation(0.5, 0.5, 1.0 - minZ));
            Assert.That(b.PositionArray.Min(p => p.Z), Is.EqualTo(1.0).Within(1e-12));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(2));
            Assert.That(TotalVolume(union), Is.EqualTo(2.0).Within(1e-9));
            Assert.That(Csg.Intersection(a, b), Is.Empty);
        }

        [Test]
        public void BoxesSharingAnEdgeStrip()
        {
            // diagonal neighbors: share exactly the edge x=1, y=1, z in 0..1;
            // their faces at x=1 / y=1 are coplanar but do not overlap
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(1, 1, 0), new V3d(2, 2, 1)));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(2));
            Assert.That(TotalVolume(union), Is.EqualTo(2.0).Within(1e-9));
            Assert.That(Csg.Intersection(a, b), Is.Empty);

            var diff = Csg.Difference(a, b);
            AssertManifold(diff);
            Assert.That(TotalVolume(diff), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void SharedVertexBoxes()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(1, 1, 1), new V3d(2, 2, 2)));
            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(2));
            Assert.That(TotalVolume(union), Is.EqualTo(2.0).Within(1e-9));
            Assert.That(Csg.Intersection(a, b), Is.Empty);
        }

        [Test]
        public void VertexPokingThroughFace()
        {
            // corner-down box whose tip penetrates the top face of a
            var cornerDown = Trafo3d.RotationEuler(Constant.PiQuarter, Fun.Atan(Constant.Sqrt2Half), 0);
            var b0 = Box(new Box3d(new V3d(-0.5), new V3d(0.5))).Transformed(cornerDown);
            var minZ = b0.PositionArray.Min(p => p.Z);
            var a = Box(Box3d.Unit);
            var b = b0.Transformed(Trafo3d.Translation(0.5, 0.5, 1.0 - minZ - 0.2)); // tip 0.2 deep

            var union = Csg.Union(a, b);
            var inter = Csg.Intersection(a, b);
            AssertManifold(union);
            AssertManifold(inter);
            var vi = TotalVolume(inter);
            Assert.That(vi, Is.GreaterThan(0.0001));
            Assert.That(TotalVolume(union) + vi, Is.EqualTo(2.0).Within(1e-9));
        }

        [Test]
        public void TouchingConfigsAreScaleAndOffsetInvariant()
        {
            foreach (var scale in new[] { 1e-3, 1.0, 1e3 })
            {
                foreach (var offset in new[] { V3d.Zero, new V3d(1e5, -1e5, 1e5) * scale })
                {
                    var trafo = Trafo3d.Scale(scale) * Trafo3d.Translation(offset);
                    var a = Box(Box3d.Unit).Transformed(trafo);
                    var b = EdgeRestingBox().Transformed(trafo);
                    var union = Csg.Union(a, b);
                    AssertManifold(union);
                    Assert.That(union.Length, Is.EqualTo(2), $"scale {scale} offset {offset}");
                    var expected = 1.6 * scale * scale * scale;
                    Assert.That(TotalVolume(union), Is.EqualTo(expected).Within(1e-6 * expected),
                        $"scale {scale} offset {offset}");
                }
            }
        }
    }
}
