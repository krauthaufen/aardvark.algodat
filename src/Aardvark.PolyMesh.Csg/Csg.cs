using System;
using System.Linq;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>Input mesh violates the contract (not watertight / not manifold / degenerate faces).</summary>
    public class CsgInputException : Exception
    {
        public CsgInputException(string message) : base(message) { }
    }

    /// <summary>Output verification failed; the result would not be manifold.</summary>
    public class CsgVerificationException : Exception
    {
        public CsgVerificationException(string message) : base(message) { }
    }

    public enum CsgVerification
    {
        /// <summary>Verify inputs only.</summary>
        InputOnly,
        /// <summary>Verify inputs and every output component (default).</summary>
        Full,
        /// <summary>Reserved; not supported in v0.</summary>
        None,
    }

    public sealed class CsgOptions
    {
        /// <summary>Scale/offset-invariant relative tolerance; see Eps.</summary>
        public double RelativeEpsilon { get; init; } = 1e-11;
        public CsgVerification Verification { get; init; } = CsgVerification.Full;

        public static readonly CsgOptions Default = new();
    }

    /// <summary>
    /// Mesh booleans on watertight manifold PolyMeshes. Results are emitted as
    /// one PolyMesh per edge-connected component. See DESIGN.md.
    /// </summary>
    public static class Csg
    {
        public static PolyMesh[] Union(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => CsgArrangement.Arrange(a, b, options).Union();

        public static PolyMesh[] Intersection(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => CsgArrangement.Arrange(a, b, options).Intersection();

        public static PolyMesh[] Difference(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => CsgArrangement.Arrange(a, b, options).Difference();

        public static PolyMesh[] Xor(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => CsgArrangement.Arrange(a, b, options).Xor();

        /// <summary>N-ary union in a single arrangement (one kernel, one subdivision — cheaper than a pairwise cascade).</summary>
        public static PolyMesh[] Union(PolyMesh[] solids, CsgOptions? options = null)
            => solids.Length == 1 ? new[] { solids[0] } : CsgArrangement.Arrange(solids, options).Union();

        /// <summary>N-ary intersection in a single arrangement.</summary>
        public static PolyMesh[] Intersection(PolyMesh[] solids, CsgOptions? options = null)
            => solids.Length == 1 ? new[] { solids[0] } : CsgArrangement.Arrange(solids, options).Intersection();

        /// <summary>a minus the union of all subtrahends, in a single arrangement.</summary>
        public static PolyMesh[] Difference(PolyMesh a, PolyMesh[] subtrahends, CsgOptions? options = null)
            => subtrahends.Length == 0
                ? new[] { a }
                : CsgArrangement.Arrange(new[] { a }.Concat(subtrahends).ToArray(), options).Difference();

        // prepared-solid (CsgMesh) forms: no re-verification/triangulation,
        // cached BVHs and ground-truth planes reused; results are prepared again

        public static CsgMesh[] Union(CsgMesh a, CsgMesh b, CsgOptions? options = null)
            => CsgArrangement.Arrange(a, b, options).UnionSolids();

        public static CsgMesh[] Intersection(CsgMesh a, CsgMesh b, CsgOptions? options = null)
            => CsgArrangement.Arrange(a, b, options).IntersectionSolids();

        public static CsgMesh[] Difference(CsgMesh a, CsgMesh b, CsgOptions? options = null)
            => CsgArrangement.Arrange(a, b, options).DifferenceSolids();

        public static CsgMesh[] Union(CsgMesh[] solids, CsgOptions? options = null)
            => solids.Length == 1 ? solids : CsgArrangement.Arrange(solids, options).UnionSolids();

        public static CsgMesh[] Intersection(CsgMesh[] solids, CsgOptions? options = null)
            => solids.Length == 1 ? solids : CsgArrangement.Arrange(solids, options).IntersectionSolids();

        public static CsgMesh[] Difference(CsgMesh a, CsgMesh[] subtrahends, CsgOptions? options = null)
            => subtrahends.Length == 0
                ? new[] { a }
                : CsgArrangement.Arrange(new[] { a }.Concat(subtrahends).ToArray(), options).DifferenceSolids();
    }
}
