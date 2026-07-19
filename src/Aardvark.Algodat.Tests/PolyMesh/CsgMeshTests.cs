using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// CsgMesh: the immutable prepared-solid type with set operators
    /// (| union, &amp; intersection, - difference, ^ symmetric difference).
    /// </summary>
    [TestFixture]
    public class CsgMeshTests
    {
        private static CsgMesh Box(Box3d b) => CsgMesh.FromPolyMesh(CsgM0Tests.QuadBox(b));

        private static void AssertManifold(CsgMesh m)
        {
            var p = m.ToPolyMesh();
            var violation = ManifoldChecks.FindManifoldViolation(
                p.FirstIndexArray, p.VertexIndexArray, p.PositionArray.Length);
            Assert.That(violation, Is.Null);
        }

        [Test]
        public void OperatorsMatchPolyMeshApi()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));

            var union = a | b;
            var inter = a & b;
            var diff = a - b;
            var xor = a ^ b;
            foreach (var m in new[] { union, inter, diff, xor }) AssertManifold(m);

            Assert.That(union.Volume, Is.EqualTo(1.875).Within(1e-9));
            Assert.That(inter.Volume, Is.EqualTo(0.125).Within(1e-9));
            Assert.That(diff.Volume, Is.EqualTo(0.875).Within(1e-9));
            Assert.That(xor.Volume, Is.EqualTo(1.75).Within(1e-9));
        }

        [Test]
        public void SetIdentityReadsCorrectly()
        {
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(0.3, 0.4, 0.2), new V3d(1.4, 1.2, 1.3)));
            // A | B == (A - B) | (B - A) | (A & B)
            var lhs = a | b;
            var rhs = (a - b) | (b - a) | (a & b);
            Assert.That(rhs.Volume, Is.EqualTo(lhs.Volume).Within(1e-9));
            AssertManifold(lhs);
        }

        [Test]
        public void ChainedOperationsStayValid()
        {
            var slab = Box(new Box3d(new V3d(0, 0, 0), new V3d(4, 2, 1)));
            var result = slab;
            for (var i = 0; i < 3; i++)
                result -= Box(new Box3d(new V3d(0.5 + i * 1.25, 0.5, -0.5), new V3d(1.0 + i * 1.25, 1.5, 1.5)));
            AssertManifold(result);
            Assert.That(result.Volume, Is.EqualTo(8.0 - 3 * 0.5).Within(1e-9));

            // chained result used as operand again
            var refill = result | Box(new Box3d(new V3d(0.5, 0.5, 0), new V3d(1.0, 1.5, 1)));
            AssertManifold(refill);
            Assert.That(refill.Volume, Is.EqualTo(8.0 - 2 * 0.5).Within(1e-9));
        }

        [Test]
        public void TouchingResultsMergeAndChain()
        {
            // union of two solids touching along an edge is a single CsgMesh
            // with two surface components; using it again must work
            var a = Box(Box3d.Unit);
            var b = Box(new Box3d(new V3d(1, 1, 0), new V3d(2, 2, 1)));
            var touching = a | b;
            Assert.That(touching.Volume, Is.EqualTo(2.0).Within(1e-9));

            var cut = touching - Box(new Box3d(new V3d(0.5, 0.5, -0.5), new V3d(1.5, 1.5, 1.5)));
            AssertManifold(cut);
            Assert.That(cut.Volume, Is.EqualTo(2.0 - 2 * 0.25).Within(1e-9));
        }

        [Test]
        public void FromPolyMeshSnapshotsInput()
        {
            var poly = CsgM0Tests.QuadBox(Box3d.Unit);
            var solid = CsgMesh.FromPolyMesh(poly);
            poly.PositionArray[0] = new V3d(1000, 1000, 1000); // mutate after the fact
            Assert.That(solid.Volume, Is.EqualTo(1.0).Within(1e-9));
            var other = Box(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            Assert.That((solid | other).Volume, Is.EqualTo(1.875).Within(1e-9));
        }

        [Test]
        public void AttributesFlowThroughOperators()
        {
            var temperature = (Symbol)"Temperature";
            var pa = CsgM0Tests.QuadBox(Box3d.Unit);
            pa.VertexAttributes[temperature] = pa.PositionArray.Map(p => (float)p.X);
            var pb = CsgM0Tests.QuadBox(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            pb.VertexAttributes[temperature] = pb.PositionArray.Map(p => (float)p.X);

            var inter = CsgMesh.FromPolyMesh(pa) & CsgMesh.FromPolyMesh(pb);
            var m = inter.ToPolyMesh();
            var temps = m.VertexAttributeArray<float>(temperature);
            Assert.That(temps, Is.Not.Null);
            for (var i = 0; i < m.PositionArray.Length; i++)
                Assert.That(temps![i], Is.EqualTo((float)m.PositionArray[i].X).Within(1e-5f));
        }

        [Test]
        public void LazyTransformIsAppliedOnUse()
        {
            var box = Box(Box3d.Unit);
            var t = Trafo3d.RotationEuler(0.3, 0.7, 0.2) * Trafo3d.Translation(0.4, 0.3, 0.5);
            var moved = box.Transformed(t);

            // O(1) transform: same result as transforming the PolyMesh up front
            var reference = CsgMesh.FromPolyMesh(CsgM0Tests.QuadBox(Box3d.Unit).Transformed(t));
            var other = Box(new Box3d(new V3d(0.5, 0.3, 0.4), new V3d(1.8, 1.9, 1.7)));
            Assert.That((moved & other).Volume, Is.EqualTo((reference & other).Volume).Within(1e-9));
            Assert.That(moved.Volume, Is.EqualTo(1.0).Within(1e-9));

            // composition
            var back = moved.Transformed(t.Inverse);
            Assert.That((back & Box(Box3d.Unit)).Volume, Is.EqualTo(1.0).Within(1e-6));

            // non-uniform scale: volume scales with the determinant
            var squashed = box.Transformed(Trafo3d.Scale(2.0, 0.5, 3.0));
            Assert.That(squashed.Volume, Is.EqualTo(3.0).Within(1e-9));
            AssertManifold(squashed - Box(new Box3d(new V3d(0.5, 0, 0), new V3d(1, 0.25, 3))));

            // ToPolyMesh materializes
            var poly = moved.ToPolyMesh();
            Assert.That(poly.PositionArray[0], Is.Not.EqualTo(box.ToPolyMesh().PositionArray[0]));

            // mirroring is rejected
            Assert.Throws<NotSupportedException>(() => box.Transformed(Trafo3d.Scale(-1.0, 1.0, 1.0)));
        }

        [Test]
        public void PreparedReuseIsFasterThanPolyMeshPath()
        {
            var pa = CsgM5Tests.Icosphere(V3d.Zero, 1.0, 5).TriangulatedCopy();
            var others = new PolyMesh[6].SetByIndex(i =>
                CsgM5Tests.Icosphere(new V3d(0.4 + 0.1 * i, 0.2, 0.1 * i), 0.8, 4));

            // warmup
            Csg.Union(pa, others[0]);

            var sw = Stopwatch.StartNew();
            foreach (var o in others) Csg.Union(pa, o);
            var cold = sw.Elapsed.TotalMilliseconds;

            var a = CsgMesh.FromPolyMesh(pa);
            var prepared = others.Map(o => CsgMesh.FromPolyMesh(o));
            sw.Restart();
            foreach (var o in prepared) Csg.Union(a, o);
            var warm = sw.Elapsed.TotalMilliseconds;

            Console.WriteLine($"PolyMesh path: {cold:0.0} ms, prepared path: {warm:0.0} ms (6 unions, 40k+10k tris)");
            Assert.That(warm, Is.LessThan(cold), "prepared solids should amortize ingest/verify/BVH");
        }
    }
}
