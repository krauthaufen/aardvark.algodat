using System;
using Aardvark.Base;

namespace Aardvark.Geometry.Csg
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
    /// relative to the magnitudes entering the computation, mirroring the
    /// floating-point error bound of the evaluated expression itself.
    /// Derived (computed) points carry a tolerance generation g and are
    /// classified with tolerance scaled by GenerationFactor^g.
    /// </summary>
    public readonly struct Eps
    {
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
        /// Classify point p against plane (unit normal n, offset d) with
        /// tolerance eps * (|p.X|+|p.Y|+|p.Z|+|d|) * GenerationFactor^generation.
        /// The tolerance scales like the fp rounding error of n·p+d, making the
        /// classification invariant under scaling and translation of the scene.
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

        private static double Gen(int g)
        {
            var f = 1.0;
            for (var i = 0; i < g; i++) f *= GenerationFactor;
            return f;
        }
    }
}
