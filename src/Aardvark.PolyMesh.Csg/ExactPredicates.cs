using System;
using System.Numerics;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Exact geometric predicates over BigInteger homogeneous coordinates.
    ///
    /// The whole scene is scaled by a single power of two into an exact integer
    /// frame: an IEEE double is exactly mantissa*2^exp, so multiplying every
    /// coordinate by 2^Shift (Shift chosen large enough to clear the most
    /// negative exponent present) turns every input coordinate into an exact
    /// integer. That global 2^Shift is a positive similarity, so it cancels in
    /// the sign of every predicate — points, planes and constructed
    /// intersections then share one exact frame with no rounding anywhere.
    ///
    /// Constructed points are NOT stored as rounded V3d; they are the exact
    /// intersection of the (exact) input planes that define them (Cramer's
    /// rule), kept in homogeneous form (X:Y:Z:W). Two constructed points are
    /// "the same" iff they are exactly equal — this replaces tolerance welding.
    ///
    /// A cheap double error-bound filter fronts every predicate; the BigInteger
    /// path only runs when the double evaluation is within its own rounding
    /// bound of zero (the rare near-degenerate case).
    /// </summary>
    internal readonly struct ExactPoint
    {
        // affine position is (X/W, Y/W, Z/W) in the scaled integer frame; W > 0.
        public readonly BigInteger X, Y, Z, W;
        public ExactPoint(BigInteger x, BigInteger y, BigInteger z, BigInteger w)
        {
            if (w.Sign < 0) { x = -x; y = -y; z = -z; w = -w; }
            X = x; Y = y; Z = z; W = w;
        }
        public bool IsFinite => !W.IsZero;
    }

    // plane A*x + B*y + C*z + D = 0 in the scaled integer frame.
    internal readonly struct ExactPlane
    {
        public readonly BigInteger A, B, C, D;
        public ExactPlane(BigInteger a, BigInteger b, BigInteger c, BigInteger d)
        { A = a; B = b; C = c; D = d; }
    }

    internal static class ExactPredicates
    {
        // --- double <-> exact integer frame ------------------------------------

        /// <summary>Decompose a finite double into mantissa*2^exp exactly.</summary>
        internal static (BigInteger mant, int exp) Frexp(double d)
        {
            if (d == 0.0) return (BigInteger.Zero, 0);
            if (double.IsNaN(d) || double.IsInfinity(d))
                throw new ArgumentException("non-finite coordinate", nameof(d));
            long bits = BitConverter.DoubleToInt64Bits(d);
            bool neg = bits < 0;
            int biasedExp = (int)((bits >> 52) & 0x7FF);
            long frac = bits & 0xF_FFFF_FFFF_FFFF;
            long mant;
            int exp;
            if (biasedExp == 0) { mant = frac; exp = -1074; }        // subnormal
            else { mant = frac | 0x10_0000_0000_0000; exp = biasedExp - 1075; }
            BigInteger m = mant;
            return (neg ? -m : m, exp);
        }

        /// <summary>
        /// The shift (a power of two) that turns every given coordinate into an
        /// exact integer: max over coords of max(0, -exp), plus the inputs' own
        /// exponents are always &gt;= -1074, so the result is bounded.
        /// </summary>
        internal static int ShiftFor(ReadOnlySpan<double> coords)
        {
            int shift = 0;
            foreach (var c in coords)
            {
                if (c == 0.0) continue;
                var (_, e) = Frexp(c);
                if (-e > shift) shift = -e;
            }
            return shift;
        }

        /// <summary>Scale a coordinate to its exact BigInteger value in the 2^shift frame.</summary>
        internal static BigInteger Scaled(double c, int shift)
        {
            var (m, e) = Frexp(c);
            int s = e + shift;                    // >= 0 by construction of shift
            return s >= 0 ? m << s : m >> (-s);   // shift chosen so s >= 0
        }

        internal static ExactPoint ToPoint(in V3d p, int shift)
            => new(Scaled(p.X, shift), Scaled(p.Y, shift), Scaled(p.Z, shift), BigInteger.One);

        /// <summary>Approximate affine position back as a double V3d (for BVH / output only).</summary>
        internal static V3d ToV3d(in ExactPoint p, int shift)
        {
            double scale = Math.Pow(2.0, -shift);
            double w = (double)p.W;
            return new V3d((double)p.X / w, (double)p.Y / w, (double)p.Z / w) * scale;
        }

        // --- exact constructions ----------------------------------------------

        /// <summary>
        /// Exact plane through three input points (affine, W==1 — planes are
        /// always defined by original polygon vertices, never by constructed
        /// points, so this is the only case that arises).
        /// </summary>
        internal static ExactPlane PlaneThrough(in ExactPoint p0, in ExactPoint p1, in ExactPoint p2)
        {
            if (!p0.W.IsOne || !p1.W.IsOne || !p2.W.IsOne)
                throw new InvalidOperationException("PlaneThrough expects affine input points");
            BigInteger ax = p1.X - p0.X, ay = p1.Y - p0.Y, az = p1.Z - p0.Z;
            BigInteger bx = p2.X - p0.X, by = p2.Y - p0.Y, bz = p2.Z - p0.Z;
            BigInteger nx = ay * bz - az * by;
            BigInteger ny = az * bx - ax * bz;
            BigInteger nz = ax * by - ay * bx;
            BigInteger d = -(nx * p0.X + ny * p0.Y + nz * p0.Z);
            return new ExactPlane(nx, ny, nz, d);
        }

        /// <summary>
        /// Exact plane from a canonical double Plane3d. The (normal, distance)
        /// doubles are converted to the same scaled integer frame; every
        /// consumer of this plane then shares one fixed exact representation, so
        /// predicates against it are mutually consistent (the property that
        /// actually removes the non-manifold), independent of the plane's own
        /// double rounding.
        /// </summary>
        internal static ExactPlane PlaneFrom(in Plane3d pl, int shift)
        {
            // Points live in the scaled frame (affine coord = real*2^shift, W=1).
            // Multiplying n.p = dist by 2^shift to integerize the normal gives
            //   (2^s n).X + ... - 2^s*dist*2^s = 0,
            // so the normal carries one 2^shift and the distance term two.
            return new ExactPlane(
                Scaled(pl.Normal.X, shift), Scaled(pl.Normal.Y, shift), Scaled(pl.Normal.Z, shift),
                Scaled(-pl.Distance, shift) << shift);
        }

        /// <summary>
        /// Exact intersection of the line through two homogeneous points with a
        /// plane: R = h_j*P_i - h_i*P_j where h = plane . point. Handles any W,
        /// so gen-2 endpoints (themselves constructed) stay exact.
        /// </summary>
        internal static ExactPoint IntersectSegmentPlane(in ExactPoint pi, in ExactPoint pj, in ExactPlane pl)
        {
            BigInteger hi = pl.A * pi.X + pl.B * pi.Y + pl.C * pi.Z + pl.D * pi.W;
            BigInteger hj = pl.A * pj.X + pl.B * pj.Y + pl.C * pj.Z + pl.D * pj.W;
            return new ExactPoint(
                hj * pi.X - hi * pj.X,
                hj * pi.Y - hi * pj.Y,
                hj * pi.Z - hi * pj.Z,
                hj * pi.W - hi * pj.W);
        }

        /// <summary>Exact intersection of three planes (homogeneous). W==0 if parallel/degenerate.</summary>
        internal static ExactPoint Intersect(in ExactPlane p, in ExactPlane q, in ExactPlane r)
        {
            // solve [A B C].(x,y,z) = -D  via Cramer; homogeneous (X:Y:Z:W).
            BigInteger W = Det3(p.A, p.B, p.C, q.A, q.B, q.C, r.A, r.B, r.C);
            BigInteger X = -Det3(p.D, p.B, p.C, q.D, q.B, q.C, r.D, r.B, r.C);
            BigInteger Y = -Det3(p.A, p.D, p.C, q.A, q.D, q.C, r.A, r.D, r.C);
            BigInteger Z = -Det3(p.A, p.B, p.D, q.A, q.B, q.D, r.A, r.B, r.D);
            return new ExactPoint(X, Y, Z, W);
        }

        // --- exact predicates --------------------------------------------------

        /// <summary>Sign of the signed distance of point to plane (exact): -1 below, 0 on, +1 above.</summary>
        internal static int SideOfPlane(in ExactPoint pt, in ExactPlane pl)
        {
            // affine: A*(X/W)+B*(Y/W)+C*(Z/W)+D = (A X+B Y+C Z+D W)/W
            BigInteger num = pl.A * pt.X + pl.B * pt.Y + pl.C * pt.Z + pl.D * pt.W;
            return num.Sign * pt.W.Sign;
        }

        /// <summary>
        /// Exact 2D orientation of three points projected by dropping one axis
        /// (0=X, 1=Y, 2=Z) — the same dominant-axis projection the double path
        /// uses, but exact because dropping a coordinate is exact. Returns
        /// +1 CCW, -1 CW, 0 collinear, in the projected coordinate order (the
        /// caller normalizes handedness).
        /// </summary>
        internal static int Orient2D(in ExactPoint a, in ExactPoint b, in ExactPoint c, int dropAxis)
        {
            (BigInteger ua, BigInteger va) = Pick(a, dropAxis);
            (BigInteger ub, BigInteger vb) = Pick(b, dropAxis);
            (BigInteger uc, BigInteger vc) = Pick(c, dropAxis);
            // affine orient2d = det[[u,v,w]] / (Wa Wb Wc); sign folds in the Ws.
            BigInteger det = Det3(ua, va, a.W, ub, vb, b.W, uc, vc, c.W);
            return det.Sign * a.W.Sign * b.W.Sign * c.W.Sign;
        }

        private static (BigInteger u, BigInteger v) Pick(in ExactPoint p, int dropAxis)
            => dropAxis == 0 ? (p.Y, p.Z) : dropAxis == 1 ? (p.X, p.Z) : (p.X, p.Y);

        /// <summary>Orientation sign of four points (exact): sign of the 4x4 homogeneous determinant.</summary>
        internal static int Orient3D(in ExactPoint a, in ExactPoint b, in ExactPoint c, in ExactPoint d)
        {
            // det of rows (Xi,Yi,Zi,Wi); divide by product of Wi to get affine sign.
            BigInteger det = Det4(
                a.X, a.Y, a.Z, a.W,
                b.X, b.Y, b.Z, b.W,
                c.X, c.Y, c.Z, c.W,
                d.X, d.Y, d.Z, d.W);
            int s = det.Sign;
            s *= a.W.Sign * b.W.Sign * c.W.Sign * d.W.Sign;
            return s;
        }

        /// <summary>Exact equality of two homogeneous points.</summary>
        internal static bool Same(in ExactPoint a, in ExactPoint b)
            => a.X * b.W == b.X * a.W
            && a.Y * b.W == b.Y * a.W
            && a.Z * b.W == b.Z * a.W;

        // --- determinants ------------------------------------------------------

        private static BigInteger Det3(
            BigInteger a, BigInteger b, BigInteger c,
            BigInteger d, BigInteger e, BigInteger f,
            BigInteger g, BigInteger h, BigInteger i)
            => a * (e * i - f * h) - b * (d * i - f * g) + c * (d * h - e * g);

        private static BigInteger Det4(
            BigInteger a0, BigInteger a1, BigInteger a2, BigInteger a3,
            BigInteger b0, BigInteger b1, BigInteger b2, BigInteger b3,
            BigInteger c0, BigInteger c1, BigInteger c2, BigInteger c3,
            BigInteger d0, BigInteger d1, BigInteger d2, BigInteger d3)
        {
            BigInteger m0 = Det3(b1, b2, b3, c1, c2, c3, d1, d2, d3);
            BigInteger m1 = Det3(b0, b2, b3, c0, c2, c3, d0, d2, d3);
            BigInteger m2 = Det3(b0, b1, b3, c0, c1, c3, d0, d1, d3);
            BigInteger m3 = Det3(b0, b1, b2, c0, c1, c2, d0, d1, d2);
            return a0 * m0 - a1 * m1 + a2 * m2 - a3 * m3;
        }
    }
}
