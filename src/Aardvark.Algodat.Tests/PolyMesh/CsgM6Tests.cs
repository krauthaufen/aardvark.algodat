using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>
    /// Polygonal (non-triangle, partly concave) input faces through real
    /// booleans, and face-vertex attribute synthesis at cut corners.
    /// </summary>
    [TestFixture]
    public class CsgM6Tests
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

        /// <summary>
        /// L-shaped prism (concave hexagon cross-section in xy, extruded in z):
        /// caps are concave polygons, sides are quads. Outer size 2x2, thickness 1, height 1.
        /// </summary>
        internal static PolyMesh LPrism()
        {
            var l = new[]
            {
                new V2d(0, 0), new V2d(2, 0), new V2d(2, 1),
                new V2d(1, 1), new V2d(1, 2), new V2d(0, 2),
            };
            var n = l.Length;
            var pos = new V3d[2 * n];
            for (var i = 0; i < n; i++)
            {
                pos[i] = new V3d(l[i].X, l[i].Y, 0);
                pos[n + i] = new V3d(l[i].X, l[i].Y, 1);
            }
            var fia = new System.Collections.Generic.List<int> { 0 };
            var via = new System.Collections.Generic.List<int>();
            void Face(params int[] idx) { via.AddRange(idx); fia.Add(via.Count); }

            Face(Enumerable.Range(0, n).Reverse().ToArray());       // bottom cap (z-)
            Face(Enumerable.Range(n, n).ToArray());                 // top cap (z+)
            for (var i = 0; i < n; i++)                             // side quads
            {
                var j = (i + 1) % n;
                Face(i, j, n + j, n + i);
            }
            return new PolyMesh
            {
                PositionArray = pos,
                FirstIndexArray = fia.ToArray(),
                VertexIndexArray = via.ToArray(),
            };
        }

        [Test]
        public void LPrismIsAValidSolid()
        {
            var l = LPrism();
            var violation = ManifoldChecks.FindManifoldViolation(
                l.FirstIndexArray, l.VertexIndexArray, l.PositionArray.Length);
            Assert.That(violation, Is.Null);
            Assert.That(Volume(l), Is.EqualTo(3.0).Within(1e-12));
        }

        [Test]
        public void LPrismBoxBooleans()
        {
            var a = LPrism();
            var b = CsgM0Tests.QuadBox(new Box3d(new V3d(0.5, 0.5, -0.5), new V3d(1.5, 1.5, 0.5)));
            var union = Csg.Union(a, b);
            var inter = Csg.Intersection(a, b);
            var diff = Csg.Difference(a, b);
            AssertManifold(union); AssertManifold(inter); AssertManifold(diff);

            // overlap: box (0.5..1.5)² x (-0.5..0.5) ∩ L-prism: the L covers
            // [0.5,1.5]x[0.5,1] plus [0.5,1]x[1,1.5] in xy → area 0.75, z-extent 0.5
            var vi = 0.75 * 0.5;
            Assert.That(TotalVolume(inter), Is.EqualTo(vi).Within(1e-9));
            Assert.That(TotalVolume(union), Is.EqualTo(3.0 + 1.0 - vi).Within(1e-9));
            Assert.That(TotalVolume(diff), Is.EqualTo(3.0 - vi).Within(1e-9));
        }

        [Test]
        public void LPrismVsRotatedLPrism()
        {
            var a = LPrism();
            var b = LPrism().Transformed(
                Trafo3d.RotationZInDegrees(90) * Trafo3d.Translation(1.75, 0.25, 0.3));
            var union = Csg.Union(a, b);
            var inter = Csg.Intersection(a, b);
            var diffAB = Csg.Difference(a, b);
            var diffBA = Csg.Difference(b, a);
            AssertManifold(union); AssertManifold(inter); AssertManifold(diffAB); AssertManifold(diffBA);
            var vi = TotalVolume(inter);
            Assert.That(TotalVolume(union) + vi, Is.EqualTo(6.0).Within(1e-9));
            Assert.That(TotalVolume(diffAB) + vi, Is.EqualTo(3.0).Within(1e-9));
            Assert.That(TotalVolume(diffBA) + vi, Is.EqualTo(3.0).Within(1e-9));
        }

        [Test]
        public void FaceVertexAttributesSynthesizedAtCuts()
        {
            // per-slot uvs encoding the source position, so synthesized values
            // are checkable: uv of any corner must equal the corner's (x, y)
            PolyMesh MakeBox(Box3d box)
            {
                var m = CsgM0Tests.QuadBox(box);
                var uvs = new V2f[m.VertexIndexArray.Length];
                for (var s = 0; s < uvs.Length; s++)
                {
                    var p = m.PositionArray[m.VertexIndexArray[s]];
                    uvs[s] = new V2f((float)p.X, (float)p.Y);
                }
                m.FaceVertexAttributes[PolyMesh.Property.DiffuseColorCoordinates] = uvs;
                return m;
            }

            var a = MakeBox(Box3d.Unit);
            var b = MakeBox(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            var result = Csg.Intersection(a, b);
            Assert.That(result.Length, Is.EqualTo(1));
            var m = result[0];
            var values = m.FaceVertexAttributeArray<V2f>(PolyMesh.Property.DiffuseColorCoordinates);
            var indices = m.FaceVertexAttributeArray<int>(-PolyMesh.Property.DiffuseColorCoordinates);
            Assert.That(values, Is.Not.Null);
            Assert.That(indices, Is.Not.Null);
            Assert.That(indices!.Length, Is.EqualTo(m.VertexIndexArray.Length));
            for (var corner = 0; corner < indices.Length; corner++)
            {
                var uv = values![indices[corner]];
                var p = m.PositionArray[m.VertexIndexArray[corner]];
                Assert.That(uv.X, Is.EqualTo((float)p.X).Within(1e-5f), $"corner {corner}");
                Assert.That(uv.Y, Is.EqualTo((float)p.Y).Within(1e-5f), $"corner {corner}");
            }
        }

        [Test]
        public void FuzzLPrismBox()
        {
            var rnd = new RandomSystem(31337);
            for (var i = 0; i < 50; i++)
            {
                var a = LPrism();
                var snap = (i & 1) == 0;
                V3d t;
                Trafo3d rot;
                if (snap)
                {
                    t = new V3d(rnd.UniformInt(9) * 0.5 - 1, rnd.UniformInt(9) * 0.5 - 1, rnd.UniformInt(5) * 0.5 - 1);
                    rot = Trafo3d.RotationZInDegrees(rnd.UniformInt(4) * 90);
                }
                else
                {
                    t = rnd.UniformV3d() * 3 - new V3d(1);
                    rot = Trafo3d.RotationEuler(
                        rnd.UniformDouble() * Constant.PiTimesTwo,
                        rnd.UniformDouble() * Constant.PiTimesTwo,
                        rnd.UniformDouble() * Constant.PiTimesTwo);
                }
                var b = CsgM0Tests.QuadBox(Box3d.Unit).Transformed(rot * Trafo3d.Translation(t));

                var va = Volume(a); var vb = Volume(b);
                var union = Csg.Union(a, b);
                var inter = Csg.Intersection(a, b);
                var diff = Csg.Difference(a, b);
                AssertManifold(union); AssertManifold(inter); AssertManifold(diff);
                var vi = TotalVolume(inter);
                Assert.That(TotalVolume(union) + vi, Is.EqualTo(va + vb).Within(1e-9 * (va + vb)), $"iteration {i}");
                Assert.That(TotalVolume(diff) + vi, Is.EqualTo(va).Within(1e-9 * (va + vb)), $"iteration {i}");
            }
        }
    }
}
