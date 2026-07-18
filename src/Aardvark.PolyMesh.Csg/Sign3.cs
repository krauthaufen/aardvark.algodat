using System;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Ternary eps-classification result. On is a first-class state:
    /// every consumer must handle all three members; switches over Sign3
    /// throw on unknown values instead of defaulting.
    /// </summary>
    public enum Sign3 : sbyte
    {
        Below = -1,
        On = 0,
        Above = 1,
    }

    /// <summary>
    /// Scale/offset-invariant tolerance model. All predicates use tolerances
    /// relative to the magnitudes entering the computation: a coordinate of
    /// magnitude m carries a semantic slop of Relative*m, and every derived
    /// quantity's tolerance is the first-order propagation of that slop.
    /// Derived (computed) points additionally carry a tolerance generation g
    /// and are classified with tolerance scaled by GenerationFactor^g.
    /// </summary>
    public readonly struct Eps
    {
        /// <summary>
        /// Relative tolerance. Default 1e-11: roughly 4-5 orders of magnitude
        /// above accumulated double rounding noise (~1e-15 relative), while
        /// still preserving micrometer detail at 10 km offsets (1e-11 * 1e7 = 1e-4 mm).
        /// </summary>
        public readonly double Relative;

        /// <summary>Per-generation tolerance growth for derived points.</summary>
        public const double GenerationFactor = 8.0;

        public Eps(double relative)
        {
            if (!(relative > 0.0) || relative >= 1e-3)
                throw new ArgumentOutOfRangeException(nameof(relative));
            Relative = relative;
        }

        /// <summary>
        /// Classify point p against plane (unit normal n, distance d) with
        /// tolerance eps * (|p.X|+|p.Y|+|p.Z|+|d|) * GenerationFactor^generation.
        /// The tolerance is the propagation of per-coordinate slop through
        /// n·p - d, making the classification invariant under scaling and
        /// translation of the whole scene.
        /// </summary>
        public Sign3 HeightSign(in Plane3d plane, in V3d p, int generation = 0)
        {
            var h = plane.Normal.Dot(p) - plane.Distance;
            var tol = Relative
                * (p.X.Abs() + p.Y.Abs() + p.Z.Abs() + plane.Distance.Abs())
                * Gen(generation);
            return h < -tol ? Sign3.Below : h > tol ? Sign3.Above : Sign3.On;
        }

        /// <summary>Coincidence test for two points (max-norm, relative).</summary>
        public bool AreCoincident(in V3d a, in V3d b, int generation = 0)
        {
            var tol = Relative * (a.NormMax + b.NormMax) * Gen(generation);
            return (a - b).NormMax <= tol;
        }

        /// <summary>
        /// Ternary orientation of 2D triangle (a,b,c): Above = counter-clockwise,
        /// Below = clockwise, On = degenerate within tolerance. The tolerance
        /// eps * (m + L) * L (m = max coordinate magnitude, L = max edge extent)
        /// is the propagation of per-coordinate slop eps*m through the
        /// determinant (a vertex moving by eps*m changes the area by ~eps*m*L),
        /// keeping the predicate scale- and offset-invariant.
        /// </summary>
        public Sign3 AreaSign(in V2d a, in V2d b, in V2d c, int generation = 0)
        {
            var d1 = b - a;
            var d2 = c - a;
            var det = d1.X * d2.Y - d1.Y * d2.X;
            var m = Fun.Max(a.X.Abs(), a.Y.Abs(), b.X.Abs(), b.Y.Abs()).Max(
                    Fun.Max(c.X.Abs(), c.Y.Abs()));
            var l = Fun.Max(d1.X.Abs(), d1.Y.Abs(), d2.X.Abs(), d2.Y.Abs());
            var tol = Relative * (m + l) * l * Gen(generation);
            return det < -tol ? Sign3.Below : det > tol ? Sign3.Above : Sign3.On;
        }

        private static double Gen(int g)
        {
            var f = 1.0;
            for (var i = 0; i < g; i++) f *= GenerationFactor;
            return f;
        }
    }
}
