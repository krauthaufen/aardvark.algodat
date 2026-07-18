using System;
using Aardvark.Base;

namespace Aardvark.Geometry.Csg
{
    /// <summary>Input mesh violates the contract (not watertight / not manifold).</summary>
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
        None,
        InputOnly,
        Full,
    }

    public sealed class CsgOptions
    {
        public double RelativeEpsilon { get; init; } = 1e-9;
        public CsgVerification Verification { get; init; } = CsgVerification.Full;

        public static readonly CsgOptions Default = new();
    }

    /// <summary>
    /// Mesh booleans on watertight manifold PolyMeshes. See DESIGN.md.
    /// </summary>
    public static class Csg
    {
        public static PolyMesh Union(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => throw new NotImplementedException("M2");

        public static PolyMesh Intersection(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => throw new NotImplementedException("M2");

        public static PolyMesh Difference(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => throw new NotImplementedException("M2");

        public static PolyMesh Xor(PolyMesh a, PolyMesh b, CsgOptions? options = null)
            => throw new NotImplementedException("M2");
    }
}
