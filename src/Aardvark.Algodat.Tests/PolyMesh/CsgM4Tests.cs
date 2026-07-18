using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// Coplanar overlapping faces: stacked solids, identical solids, and
    /// partially shared face planes (OnSame/OnOpposite handling).
    /// </summary>
    [TestFixture]
    public class CsgM4Tests
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
        public void StackedBoxesFullFaceContact()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0, 0, 1), new V3d(1, 1, 2)));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1)); // tower: contact face vanishes
            Assert.That(TotalVolume(union), Is.EqualTo(2.0).Within(1e-9));

            Assert.That(Csg.Intersection(a, b), Is.Empty); // measure-zero contact

            var diff = Csg.Difference(a, b);
            AssertManifold(diff);
            Assert.That(TotalVolume(diff), Is.EqualTo(1.0).Within(1e-9)); // b only touches a

            var xor = Csg.Xor(a, b);
            AssertManifold(xor);
            Assert.That(xor.Length, Is.EqualTo(2)); // both solids, separate
            Assert.That(TotalVolume(xor), Is.EqualTo(2.0).Within(1e-9));
        }

        [Test]
        public void StackedBoxesPartialFaceContact()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0, 1), new V3d(1.5, 1, 2)));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(2.0).Within(1e-9));

            Assert.That(Csg.Intersection(a, b), Is.Empty);

            var diff = Csg.Difference(a, b);
            AssertManifold(diff);
            Assert.That(TotalVolume(diff), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void IdenticalBoxes()
        {
            var a = Box(Box3d.Unit);
            var b = Box(Box3d.Unit);

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(1.0).Within(1e-9));

            var inter = Csg.Intersection(a, b);
            AssertManifold(inter);
            Assert.That(TotalVolume(inter), Is.EqualTo(1.0).Within(1e-9));

            Assert.That(Csg.Difference(a, b), Is.Empty);
            Assert.That(Csg.Xor(a, b), Is.Empty);
        }

        [Test]
        public void SharedTopAndBottomPlanes()
        {
            // b overlaps a and shares both the z=0 and z=1 planes (same winding)
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0), new V3d(1.5, 1.5, 1)));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(1.75).Within(1e-9));

            var inter = Csg.Intersection(a, b);
            AssertManifold(inter);
            Assert.That(TotalVolume(inter), Is.EqualTo(0.25).Within(1e-9));

            var diff = Csg.Difference(a, b);
            AssertManifold(diff);
            Assert.That(TotalVolume(diff), Is.EqualTo(0.75).Within(1e-9));
        }

        [Test]
        public void SlidingBoxesFourSharedPlanes()
        {
            // b is a shifted a: four side planes coincide, interiors overlap in x
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0, 0), new V3d(1.5, 1, 1)));

            var union = Csg.Union(a, b);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(1.5).Within(1e-9));

            var inter = Csg.Intersection(a, b);
            AssertManifold(inter);
            Assert.That(TotalVolume(inter), Is.EqualTo(0.5).Within(1e-9));

            var diff = Csg.Difference(a, b);
            AssertManifold(diff);
            Assert.That(TotalVolume(diff), Is.EqualTo(0.5).Within(1e-9));

            var xor = Csg.Xor(a, b);
            AssertManifold(xor);
            Assert.That(TotalVolume(xor), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void CoplanarConfigsAreScaleAndOffsetInvariant()
        {
            foreach (var scale in new[] { 1e-3, 1.0, 1e3 })
            {
                foreach (var offset in new[] { V3d.Zero, new V3d(1e5, -1e5, 1e5) * scale })
                {
                    var trafo = Trafo3d.Scale(scale) * Trafo3d.Translation(offset);
                    var a = Box(Box3d.Unit).Transformed(trafo);
                    var b = Box(new Box3d(new V3d(0.5, 0, 0), new V3d(1.5, 1, 1))).Transformed(trafo);
                    var union = Csg.Union(a, b);
                    AssertManifold(union);
                    var expected = 1.5 * scale * scale * scale;
                    Assert.That(union.Length, Is.EqualTo(1), $"scale {scale} offset {offset}");
                    Assert.That(TotalVolume(union), Is.EqualTo(expected).Within(1e-6 * expected),
                        $"scale {scale} offset {offset}");
                }
            }
        }
    }
}
