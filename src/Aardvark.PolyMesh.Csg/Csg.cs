using System;
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
    }
}
