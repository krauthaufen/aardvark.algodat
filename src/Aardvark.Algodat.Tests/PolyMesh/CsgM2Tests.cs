using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    [TestFixture]
    public class CsgM2Tests
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
        public void OverlappingBoxesUnion()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            var result = Csg.Union(a, b);
            AssertManifold(result);
            Assert.That(result.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(result), Is.EqualTo(1.875).Within(1e-9));
        }

        [Test]
        public void OverlappingBoxesIntersection()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            var result = Csg.Intersection(a, b);
            AssertManifold(result);
            Assert.That(result.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(result), Is.EqualTo(0.125).Within(1e-9));
        }

        [Test]
        public void OverlappingBoxesDifference()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            var result = Csg.Difference(a, b);
            AssertManifold(result);
            Assert.That(result.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(result), Is.EqualTo(0.875).Within(1e-9));
        }

        [Test]
        public void OverlappingBoxesXor()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            var result = Csg.Xor(a, b);
            AssertManifold(result);
            Assert.That(TotalVolume(result), Is.EqualTo(1.75).Within(1e-9));
        }

        [Test]
        public void VolumeConservation()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.3, 0.4, 0.5), new V3d(1.2, 1.6, 1.4)));
            var union = TotalVolume(Csg.Union(a, b));
            var inter = TotalVolume(Csg.Intersection(a, b));
            Assert.That(union + inter, Is.EqualTo(Volume(a) + Volume(b)).Within(1e-9));
        }

        [Test]
        public void ContainedBoxOperations()
        {
            var inner = Box(new Box3d(new V3d(0.25, 0.25, 0.25), new V3d(0.75, 0.75, 0.75)));
            var outer = Box(Box3d.Unit);

            var union = Csg.Union(inner, outer);
            AssertManifold(union);
            Assert.That(union.Length, Is.EqualTo(1));
            Assert.That(TotalVolume(union), Is.EqualTo(1.0).Within(1e-9));

            var inter = Csg.Intersection(inner, outer);
            AssertManifold(inter);
            Assert.That(TotalVolume(inter), Is.EqualTo(0.125).Within(1e-9));

            Assert.That(Csg.Difference(inner, outer), Is.Empty);

            var shell = Csg.Difference(outer, inner);
            AssertManifold(shell);
            Assert.That(shell.Length, Is.EqualTo(2)); // outer boundary + flipped cavity
            Assert.That(TotalVolume(shell), Is.EqualTo(0.875).Within(1e-9));

            var xor = Csg.Xor(outer, inner);
            AssertManifold(xor);
            Assert.That(TotalVolume(xor), Is.EqualTo(0.875).Within(1e-9));
        }

        [Test]
        public void RotatedBoxBooleans()
        {
            var a = Box(Box3d.Unit);
            var trafo = Trafo3d.RotationEuler(0.31, 0.73, 0.21) * Trafo3d.Translation(0.4, 0.35, 0.45);
            var b = Box(Box3d.Unit).Transformed(trafo);

            var union = Csg.Union(a, b);
            var inter = Csg.Intersection(a, b);
            var diffAB = Csg.Difference(a, b);
            var diffBA = Csg.Difference(b, a);
            AssertManifold(union); AssertManifold(inter); AssertManifold(diffAB); AssertManifold(diffBA);

            var vu = TotalVolume(union);
            var vi = TotalVolume(inter);
            Assert.That(vi, Is.GreaterThan(0.01));
            Assert.That(vu + vi, Is.EqualTo(2.0).Within(1e-9));
            Assert.That(TotalVolume(diffAB) + vi, Is.EqualTo(1.0).Within(1e-9));
            Assert.That(TotalVolume(diffBA) + vi, Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void BooleansAreOffsetInvariant()
        {
            foreach (var offset in new[] { V3d.Zero, new V3d(1e6, -1e6, 1e6) })
            {
                var a = Box(Box3d.Unit.Translated(offset));
                var b = Box(new Box3d(new V3d(0.5, 0.5, 0.5) + offset, new V3d(1.5, 1.5, 1.5) + offset));
                var union = Csg.Union(a, b);
                AssertManifold(union);
                Assert.That(union.Length, Is.EqualTo(1), $"offset {offset}");
                Assert.That(TotalVolume(union), Is.EqualTo(1.875).Within(1e-3), $"offset {offset}");
            }
        }

        [Test]
        public void CutVertexAttributesAreInterpolated()
        {
            var temperature = (Symbol)"Temperature";
            var a = Box(Box3d.Unit);
            a.VertexAttributes[temperature] = a.PositionArray.Map(p => (float)p.X);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            b.VertexAttributes[temperature] = b.PositionArray.Map(p => (float)p.X);

            var result = Csg.Intersection(a, b);
            Assert.That(result.Length, Is.EqualTo(1));
            var m = result[0];
            var temps = m.VertexAttributeArray<float>(temperature);
            Assert.That(temps, Is.Not.Null);
            for (var i = 0; i < m.PositionArray.Length; i++)
                Assert.That(temps![i], Is.EqualTo((float)m.PositionArray[i].X).Within(1e-5f),
                    $"temperature at vertex {i} should equal its x coordinate");
        }

        [Test]
        public void CoplanarFacesThrowNotImplemented()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0.0), new V3d(1.5, 1.5, 1.0))); // shares z=0 and z=1 planes
            Assert.Throws<NotImplementedException>(() => Csg.Union(a, b));
        }

        [Test]
        public void ResultsAreDeterministic()
        {
            var a = Box(Box3d.Unit);
            var trafo = Trafo3d.RotationEuler(0.31, 0.73, 0.21) * Trafo3d.Translation(0.4, 0.35, 0.45);
            var b = Box(Box3d.Unit).Transformed(trafo);
            var r1 = Csg.Union(a, b);
            var r2 = Csg.Union(a, b);
            Assert.That(r1.Length, Is.EqualTo(r2.Length));
            for (var i = 0; i < r1.Length; i++)
            {
                Assert.That(r1[i].VertexIndexArray, Is.EqualTo(r2[i].VertexIndexArray));
                Assert.That(r1[i].PositionArray, Is.EqualTo(r2[i].PositionArray));
            }
        }
    }
}
