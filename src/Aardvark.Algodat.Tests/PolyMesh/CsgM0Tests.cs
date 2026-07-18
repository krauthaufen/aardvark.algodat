using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    [TestFixture]
    public class CsgM0Tests
    {
        #region fixtures

        /// <summary>Hand-built axis-aligned box as 6 outward-wound quads.</summary>
        internal static PolyMesh QuadBox(Box3d box)
        {
            var n = box.Min; var x = box.Max;
            var positions = new[]
            {
                new V3d(n.X, n.Y, n.Z), new V3d(x.X, n.Y, n.Z),
                new V3d(x.X, x.Y, n.Z), new V3d(n.X, x.Y, n.Z),
                new V3d(n.X, n.Y, x.Z), new V3d(x.X, n.Y, x.Z),
                new V3d(x.X, x.Y, x.Z), new V3d(n.X, x.Y, x.Z),
            };
            var quads = new[]
            {
                3, 2, 1, 0, // bottom (z-)
                4, 5, 6, 7, // top (z+)
                0, 1, 5, 4, // y-
                2, 3, 7, 6, // y+
                1, 2, 6, 5, // x+
                3, 0, 4, 7, // x-
            };
            return new PolyMesh
            {
                PositionArray = positions,
                FirstIndexArray = new[] { 0, 4, 8, 12, 16, 20, 24 },
                VertexIndexArray = quads,
            };
        }

        /// <summary>Signed volume via divergence theorem, centroid-relative for offset robustness. Faces are fan-triangulated.</summary>
        internal static double Volume(PolyMesh m)
        {
            var fia = m.FirstIndexArray; var via = m.VertexIndexArray; var pos = m.PositionArray;
            var centroid = pos.Aggregate(V3d.Zero, (s, p) => s + p) / pos.Length;
            var sum = 0.0;
            for (var fi = 0; fi + 1 < fia.Length; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                var p0 = pos[via[start]] - centroid;
                for (var i = start + 1; i + 1 < end; i++)
                {
                    var p1 = pos[via[i]] - centroid;
                    var p2 = pos[via[i + 1]] - centroid;
                    sum += p0.Dot(p1.Cross(p2));
                }
            }
            return sum / 6.0;
        }

        private static void AssertManifold(PolyMesh m)
        {
            var violation = ManifoldChecks.FindManifoldViolation(
                m.FirstIndexArray, m.VertexIndexArray, m.PositionArray.Length);
            Assert.That(violation, Is.Null);
        }

        #endregion

        #region Eps predicates

        [Test]
        public void HeightSignBasics()
        {
            var eps = new Eps(1e-11);
            var plane = new Plane3d(V3d.ZAxis, 0.0);
            Assert.That(eps.HeightSign(plane, new V3d(0.3, 0.2, 1e-6)), Is.EqualTo(Sign3.Above));
            Assert.That(eps.HeightSign(plane, new V3d(0.3, 0.2, -1e-6)), Is.EqualTo(Sign3.Below));
            Assert.That(eps.HeightSign(plane, new V3d(0.3, 0.2, 1e-13)), Is.EqualTo(Sign3.On));
            Assert.That(eps.HeightSign(plane, new V3d(0.3, 0.2, -1e-13)), Is.EqualTo(Sign3.On));
        }

        [Test]
        public void HeightSignIsScaleInvariant()
        {
            var eps = new Eps(1e-11);
            var heights = new[] { -1e-6, -1e-13, 0.0, 1e-13, 1e-6 };
            var baseline = heights.Map(h => eps.HeightSign(new Plane3d(V3d.ZAxis, 0.0), new V3d(0.3, 0.2, h)));
            foreach (var s in new[] { 1e-6, 1e-3, 1e3, 1e6 })
            {
                var scaled = heights.Map(h => eps.HeightSign(new Plane3d(V3d.ZAxis, 0.0), new V3d(0.3 * s, 0.2 * s, h * s)));
                Assert.That(scaled, Is.EqualTo(baseline), $"scale {s}");
            }
        }

        [Test]
        public void HeightSignSurvivesOffsetForResolvableFeatures()
        {
            var eps = new Eps(1e-11);
            // a height of 1 is far above the noise floor even at offset 1e7
            foreach (var offset in new[] { V3d.Zero, new V3d(1e7, 1e7, 1e7) })
            {
                var plane = new Plane3d(V3d.ZAxis, offset.Z);
                Assert.That(eps.HeightSign(plane, offset + new V3d(0.3, 0.2, 1.0)), Is.EqualTo(Sign3.Above));
                Assert.That(eps.HeightSign(plane, offset + new V3d(0.3, 0.2, -1.0)), Is.EqualTo(Sign3.Below));
                Assert.That(eps.HeightSign(plane, offset + new V3d(0.3, 0.2, 0.0)), Is.EqualTo(Sign3.On));
            }
            // by design, a feature below the noise floor of a far-offset frame degrades to On
            var far = new Plane3d(V3d.ZAxis, 1e7);
            Assert.That(eps.HeightSign(far, new V3d(1e7, 1e7, 1e7 + 1e-6)), Is.EqualTo(Sign3.On));
        }

        [Test]
        public void AreaSignBasicsAndScaleInvariance()
        {
            var eps = new Eps(1e-11);
            Assert.That(eps.AreaSign(new V2d(0, 0), new V2d(1, 0), new V2d(0, 1)), Is.EqualTo(Sign3.Above));
            Assert.That(eps.AreaSign(new V2d(0, 0), new V2d(0, 1), new V2d(1, 0)), Is.EqualTo(Sign3.Below));
            Assert.That(eps.AreaSign(new V2d(0, 0), new V2d(1, 0), new V2d(2, 1e-13)), Is.EqualTo(Sign3.On));
            foreach (var s in new[] { 1e-6, 1e6 })
            {
                Assert.That(eps.AreaSign(new V2d(0, 0), new V2d(s, 0), new V2d(0, s)), Is.EqualTo(Sign3.Above), $"scale {s}");
                Assert.That(eps.AreaSign(new V2d(0, 0), new V2d(s, 0), new V2d(2 * s, s * 1e-13)), Is.EqualTo(Sign3.On), $"scale {s}");
            }
        }

        #endregion

        #region triangulation

        [Test]
        public void EarClipConcavePolygon()
        {
            // L-shape, counter-clockwise
            var p = new[]
            {
                new V2d(0, 0), new V2d(2, 0), new V2d(2, 1),
                new V2d(1, 1), new V2d(1, 2), new V2d(0, 2),
            };
            var tris = new List<(int, int, int)>();
            Assert.That(Triangulator.EarClip(p, new Eps(1e-11), tris), Is.True);
            Assert.That(tris.Count, Is.EqualTo(4));
            var area = tris.Sum(t =>
            {
                var (a, b, c) = t;
                return 0.5 * ((p[b].X - p[a].X) * (p[c].Y - p[a].Y) - (p[b].Y - p[a].Y) * (p[c].X - p[a].X));
            });
            Assert.That(area, Is.EqualTo(3.0).Within(1e-12)); // all CCW and covering the L exactly
        }

        [Test]
        public void EarClipDropsCollinearVertices()
        {
            // square with a redundant collinear vertex on the bottom edge
            var p = new[]
            {
                new V2d(0, 0), new V2d(1, 0), new V2d(2, 0), new V2d(2, 2), new V2d(0, 2),
            };
            var tris = new List<(int, int, int)>();
            Assert.That(Triangulator.EarClip(p, new Eps(1e-11), tris), Is.True);
            var area = tris.Sum(t =>
            {
                var (a, b, c) = t;
                return 0.5 * ((p[b].X - p[a].X) * (p[c].Y - p[a].Y) - (p[b].Y - p[a].Y) * (p[c].X - p[a].X));
            });
            Assert.That(area, Is.EqualTo(4.0).Within(1e-12));
            foreach (var (a, b, c) in tris)
                Assert.That(new Eps(1e-11).AreaSign(p[a], p[b], p[c]), Is.EqualTo(Sign3.Above));
        }

        #endregion

        #region input verification

        [Test]
        public void OpenMeshIsRejected()
        {
            var box = QuadBox(Box3d.Unit);
            var open = new PolyMesh
            {
                PositionArray = box.PositionArray,
                FirstIndexArray = box.FirstIndexArray.Take(6).ToArray(), // drop last face
                VertexIndexArray = box.VertexIndexArray.Take(20).ToArray(),
            };
            var other = QuadBox(Box3d.Unit.Translated(new V3d(10, 0, 0)));
            var ex = Assert.Throws<CsgInputException>(() => Csg.Union(open, other));
            Assert.That(ex!.Message, Does.Contain("no opposite"));
        }

        [Test]
        public void InconsistentWindingIsRejected()
        {
            var box = QuadBox(Box3d.Unit);
            var via = box.VertexIndexArray.Copy();
            (via[0], via[3]) = (via[3], via[0]);
            (via[1], via[2]) = (via[2], via[1]); // reverse first quad
            var bad = new PolyMesh
            {
                PositionArray = box.PositionArray,
                FirstIndexArray = box.FirstIndexArray,
                VertexIndexArray = via,
            };
            var other = QuadBox(Box3d.Unit.Translated(new V3d(10, 0, 0)));
            var ex = Assert.Throws<CsgInputException>(() => Csg.Union(bad, other));
            Assert.That(ex!.Message, Does.Contain("occurs twice"));
        }

        [Test]
        public void BowtieVertexIsRejected()
        {
            // two tetrahedra sharing exactly one vertex (edge-manifold but not vertex-manifold)
            var p = new List<V3d>
            {
                new(0, 0, 0), // shared apex
                new(1, 0, -1), new(0, 1, -1), new(-1, -1, -1),
                new(1, 0, 1), new(0, 1, 1), new(-1, -1, 1),
            };
            var faces = new List<int[]>
            {
                new[] { 1, 2, 3 }, new[] { 0, 2, 1 }, new[] { 0, 3, 2 }, new[] { 0, 1, 3 },
                new[] { 4, 5, 6 }, new[] { 0, 4, 5 }, new[] { 0, 5, 6 }, new[] { 0, 6, 4 },
            };
            // fix winding of lower tet (base must face away): base 1,2,3 with apex above → wound consistently below
            var fia = new int[faces.Count + 1];
            for (var i = 0; i < faces.Count; i++) fia[i + 1] = fia[i] + faces[i].Length;
            var mesh = new PolyMesh
            {
                PositionArray = p.ToArray(),
                FirstIndexArray = fia,
                VertexIndexArray = faces.SelectMany(f => f).ToArray(),
            };
            var other = QuadBox(Box3d.Unit.Translated(new V3d(10, 0, 0)));
            var ex = Assert.Throws<CsgInputException>(() => Csg.Union(mesh, other));
            Assert.That(ex!.Message, Does.Contain("disconnected link").Or.Contain("no opposite").Or.Contain("occurs twice"));
        }

        [Test]
        public void NonPlanarFaceIsRejected()
        {
            var box = QuadBox(Box3d.Unit);
            var pos = box.PositionArray.Copy();
            pos[6] += new V3d(0, 0, 0.1); // bend the top quad
            var bad = new PolyMesh
            {
                PositionArray = pos,
                FirstIndexArray = box.FirstIndexArray,
                VertexIndexArray = box.VertexIndexArray,
            };
            var other = QuadBox(Box3d.Unit.Translated(new V3d(10, 0, 0)));
            var ex = Assert.Throws<CsgInputException>(() => Csg.Union(bad, other));
            Assert.That(ex!.Message, Does.Contain("not planar"));
        }

        #endregion

        #region disjoint booleans

        [Test]
        public void DisjointUnionEmitsTwoManifoldComponents()
        {
            var a = QuadBox(Box3d.Unit);
            var b = QuadBox(new Box3d(new V3d(10, 0, 0), new V3d(12, 3, 4)));
            var result = Csg.Union(a, b);
            Assert.That(result.Length, Is.EqualTo(2));
            foreach (var m in result)
            {
                AssertManifold(m);
                Assert.That(m.FaceVertexCountRange.Max, Is.EqualTo(3));
                Assert.That(m.FirstIndexArray.Length - 1, Is.EqualTo(12));
            }
            var volumes = result.Map(Volume).OrderBy(v => v).ToArray();
            Assert.That(volumes[0], Is.EqualTo(1.0).Within(1e-9));
            Assert.That(volumes[1], Is.EqualTo(24.0).Within(1e-9));
        }

        [Test]
        public void DisjointIntersectionIsEmpty()
        {
            var a = QuadBox(Box3d.Unit);
            var b = QuadBox(new Box3d(new V3d(10, 0, 0), new V3d(11, 1, 1)));
            Assert.That(Csg.Intersection(a, b), Is.Empty);
        }

        [Test]
        public void DisjointDifferenceReturnsA()
        {
            var a = QuadBox(Box3d.Unit);
            var b = QuadBox(new Box3d(new V3d(10, 0, 0), new V3d(11, 1, 1)));
            var result = Csg.Difference(a, b);
            Assert.That(result.Length, Is.EqualTo(1));
            AssertManifold(result[0]);
            Assert.That(Volume(result[0]), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void DisjointUnionIsOffsetInvariant()
        {
            foreach (var offset in new[] { V3d.Zero, new V3d(1e7, -1e7, 1e7) })
            {
                var a = QuadBox(Box3d.Unit.Translated(offset));
                var b = QuadBox(new Box3d(new V3d(10, 0, 0) + offset, new V3d(11, 1, 1) + offset));
                var result = Csg.Union(a, b);
                Assert.That(result.Length, Is.EqualTo(2), $"offset {offset}");
                foreach (var m in result) AssertManifold(m);
                var volumes = result.Map(Volume).OrderBy(v => v).ToArray();
                Assert.That(volumes[0], Is.EqualTo(1.0).Within(1e-4), $"offset {offset}");
                Assert.That(volumes[1], Is.EqualTo(1.0).Within(1e-4), $"offset {offset}");
            }
        }

        #endregion

        #region attributes

        [Test]
        public void VertexAndFaceAttributesSurviveDisjointUnion()
        {
            var temperature = (Symbol)"Temperature";
            var materialId = (Symbol)"MaterialId";

            PolyMesh MakeBox(Box3d box, float tBase, int mat)
            {
                var m = QuadBox(box);
                m.VertexAttributes[temperature] = new float[8].SetByIndex(i => tBase + i);
                m.FaceAttributes[materialId] = new int[6].SetByIndex(_ => mat);
                return m;
            }

            var a = MakeBox(Box3d.Unit, 100f, 1);
            var b = MakeBox(new Box3d(new V3d(10, 0, 0), new V3d(11, 1, 1)), 200f, 2);
            var result = Csg.Union(a, b);
            Assert.That(result.Length, Is.EqualTo(2));

            foreach (var m in result)
            {
                var temps = m.VertexAttributeArray<float>(temperature);
                var mats = m.FaceAttributeArray<int>(materialId);
                Assert.That(temps, Is.Not.Null);
                Assert.That(temps!.Length, Is.EqualTo(m.PositionArray.Length));
                Assert.That(mats, Is.Not.Null);
                Assert.That(mats!.Length, Is.EqualTo(m.FirstIndexArray.Length - 1));
                var fromA = m.PositionArray[0].X < 5;
                Assert.That(temps.All(t => fromA ? t < 150 : t >= 150), Is.True);
                Assert.That(mats.All(x => x == (fromA ? 1 : 2)), Is.True);
            }
        }

        [Test]
        public void FaceVertexAttributesResolveToSourceSlots()
        {
            // give every face-vertex slot a unique uv encoding (slot, meshTag);
            // after the boolean each output corner must resolve to the uv of a
            // slot whose source position equals the corner's position
            PolyMesh MakeBox(Box3d box, float tag)
            {
                var m = QuadBox(box);
                m.FaceVertexAttributes[PolyMesh.Property.DiffuseColorCoordinates] =
                    new V2f[24].SetByIndex(i => new V2f(i, tag));
                return m;
            }

            var a = MakeBox(Box3d.Unit, 0f);
            var b = MakeBox(new Box3d(new V3d(10, 0, 0), new V3d(11, 1, 1)), 1f);
            var sources = new[] { a, b };
            var result = Csg.Union(a, b);
            Assert.That(result.Length, Is.EqualTo(2));

            foreach (var m in result)
            {
                var values = m.FaceVertexAttributeArray<V2f>(PolyMesh.Property.DiffuseColorCoordinates);
                var indices = m.FaceVertexAttributeArray<int>(-PolyMesh.Property.DiffuseColorCoordinates);
                Assert.That(values, Is.Not.Null);
                Assert.That(indices, Is.Not.Null);
                Assert.That(indices!.Length, Is.EqualTo(m.VertexIndexArray.Length));
                for (var corner = 0; corner < indices.Length; corner++)
                {
                    var uv = values![indices[corner]];
                    var source = sources[(int)uv.Y];
                    var slot = (int)uv.X;
                    var sourcePosition = source.PositionArray[source.VertexIndexArray[slot]];
                    var cornerPosition = m.PositionArray[m.VertexIndexArray[corner]];
                    Assert.That(cornerPosition, Is.EqualTo(sourcePosition));
                }
            }
        }

        #endregion
    }
}
