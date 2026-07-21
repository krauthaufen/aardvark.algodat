using System;
using System.Numerics;
using Aardvark.Base;
using Aardvark.Geometry;
using NUnit.Framework;

namespace Aardvark.Geometry.Tests
{
    // Phase 1 of the exact-predicates milestone: validate the exact number
    // kernel (BigInteger homogeneous coordinates) in isolation, before it is
    // threaded into the arrangement pipeline.
    [TestFixture]
    public class ExactPredicatesTests
    {
        static int Shift(params V3d[] pts)
        {
            var cs = new double[pts.Length * 3];
            for (int i = 0; i < pts.Length; i++)
            { cs[3 * i] = pts[i].X; cs[3 * i + 1] = pts[i].Y; cs[3 * i + 2] = pts[i].Z; }
            return ExactPredicates.ShiftFor(cs);
        }

        static ExactPlane Plane(int shift, V3d a, V3d b, V3d c)
            => ExactPredicates.PlaneThrough(
                ExactPredicates.ToPoint(a, shift),
                ExactPredicates.ToPoint(b, shift),
                ExactPredicates.ToPoint(c, shift));

        [Test]
        public void Frexp_RoundTrips()
        {
            foreach (var d in new[] { 0.0, 1.0, -1.0, 0.1, 1e7 + 0.5, -3.25, double.Epsilon, 1e-300, 12345.678 })
            {
                var (m, e) = ExactPredicates.Frexp(d);
                // reconstruct m * 2^e exactly and compare
                double r = e >= 0 ? (double)(m << e) : (double)m * Math.Pow(2.0, e);
                Assert.That(r, Is.EqualTo(d).Within(Math.Abs(d) * 1e-15 + double.Epsilon), $"frexp {d}");
            }
        }

        [Test]
        public void ConstructedPoint_IsExactlyOnItsDefiningPlanes()
        {
            // three tilted planes with independent normals, offset far from the
            // origin so the double evaluation loses precision (a "10 km scene").
            var t = new V3d(1e7, 1e7, 1e7);
            var pts = new[]
            {
                // plane A: base + edges (1,0,0.5),(0,1,0.25) -> normal (-0.5,-0.25,1)
                t + new V3d(0.5, 0.25, 0.75), t + new V3d(1.5, 0.25, 1.25), t + new V3d(0.5, 1.25, 1.0),
                // plane B: base + edges (1,0.5,0),(0,0.25,1) -> normal (0.5,-1,0.25)
                t + new V3d(0.25, 0.5, 0.5), t + new V3d(1.25, 1.0, 0.5), t + new V3d(0.25, 0.75, 1.5),
                // plane C: z = 0.25 (normal (0,0,1))
                t + new V3d(0.75, 0.5, 0.25), t + new V3d(1.75, 0.5, 0.25), t + new V3d(0.75, 1.5, 0.25),
            };
            int sh = Shift(pts);
            var p = Plane(sh, pts[0], pts[1], pts[2]);
            var q = Plane(sh, pts[3], pts[4], pts[5]);
            var r = Plane(sh, pts[6], pts[7], pts[8]);

            var x = ExactPredicates.Intersect(p, q, r);
            Assert.That(x.IsFinite, Is.True, "planes must meet in a point");

            // definitional: the intersection lies exactly on all three planes.
            Assert.That(ExactPredicates.SideOfPlane(x, p), Is.EqualTo(0), "on plane p");
            Assert.That(ExactPredicates.SideOfPlane(x, q), Is.EqualTo(0), "on plane q");
            Assert.That(ExactPredicates.SideOfPlane(x, r), Is.EqualTo(0), "on plane r");

            // and the rounded double position generally does NOT certify on-ness:
            // this is exactly the rounding the exact path removes.
            var xd = ExactPredicates.ToV3d(x, sh);
            var pd = Plane3dThrough(pts[0], pts[1], pts[2]);
            double residual = pd.Normal.Dot(xd) - pd.Distance;
            TestContext.WriteLine($"double residual at reconstructed point: {residual:E3} (exact says 0)");
        }

        [Test]
        public void SideOfPlane_SignsAreCorrect()
        {
            int sh = Shift(new V3d(0, 0, 0), new V3d(10, 0, 0), new V3d(0, 10, 0), new V3d(0, 0, 5), new V3d(0, 0, -5));
            var z0 = Plane(sh, new V3d(0, 0, 0), new V3d(10, 0, 0), new V3d(0, 10, 0)); // z = 0, normal +z
            var above = ExactPredicates.ToPoint(new V3d(1, 1, 5), sh);
            var below = ExactPredicates.ToPoint(new V3d(1, 1, -5), sh);
            var on = ExactPredicates.ToPoint(new V3d(3, 7, 0), sh);
            int sa = ExactPredicates.SideOfPlane(above, z0);
            int sb = ExactPredicates.SideOfPlane(below, z0);
            Assert.That(ExactPredicates.SideOfPlane(on, z0), Is.EqualTo(0), "on z=0");
            Assert.That(sa, Is.Not.EqualTo(0));
            Assert.That(sb, Is.EqualTo(-sa), "opposite sides have opposite sign");
        }

        [Test]
        public void Orient3D_ExactlyZeroForCoplanar_NonzeroOtherwise()
        {
            // non-degenerate triangle in z=0 (b spans +x, c spans +y), far offset.
            V3d va = new(1e7, 1e7, 0), vb = new(1e7 + 1.0001, 1e7 + 3, 0), vc = new(1e7 + 5, 1e7 + 1e4 + 2, 0);
            V3d vOn = new(1e7 + 7, 1e7 + 7, 0), vOff = new(1e7 + 7, 1e7 + 7, 1);
            int sh = Shift(va, vb, vc, vOn, vOff);
            var a = ExactPredicates.ToPoint(va, sh);
            var b = ExactPredicates.ToPoint(vb, sh);
            var c = ExactPredicates.ToPoint(vc, sh);
            var dOn = ExactPredicates.ToPoint(vOn, sh);   // z=0 -> coplanar
            var dOff = ExactPredicates.ToPoint(vOff, sh); // off z=0
            Assert.That(ExactPredicates.Orient3D(a, b, c, dOn), Is.EqualTo(0), "coplanar -> 0");
            Assert.That(ExactPredicates.Orient3D(a, b, c, dOff), Is.Not.EqualTo(0), "off-plane -> nonzero");
        }

        [Test]
        public void Same_DetectsIdentityUpToHomogeneousScale()
        {
            var a = new ExactPoint(new BigInteger(6), new BigInteger(9), new BigInteger(3), new BigInteger(3));
            var b = new ExactPoint(new BigInteger(2), new BigInteger(3), new BigInteger(1), new BigInteger(1));
            var c = new ExactPoint(new BigInteger(2), new BigInteger(3), new BigInteger(2), new BigInteger(1));
            Assert.That(ExactPredicates.Same(a, b), Is.True, "same affine point at different scale");
            Assert.That(ExactPredicates.Same(a, c), Is.False, "distinct points");
        }

        static Plane3d Plane3dThrough(V3d a, V3d b, V3d c)
        {
            var n = Vec.Cross(b - a, c - a).Normalized;
            return new Plane3d(n, Vec.Dot(n, a));
        }
    }
}
