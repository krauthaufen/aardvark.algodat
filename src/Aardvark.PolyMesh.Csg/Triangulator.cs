using System;
using System.Collections.Generic;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Ear-clipping triangulation of planar polygons, driven by the ternary
    /// AreaSign predicate. Zero-area (On) ears are removed without emitting a
    /// triangle, so collinear polygon vertices never produce degenerate output.
    /// </summary>
    internal static class Triangulator
    {
        /// <summary>
        /// Computes the unit-normal plane of a polygon via Newell's method,
        /// evaluated on centroid-relative coordinates so the result does not
        /// degrade with scene offset.
        /// </summary>
        public static Plane3d NewellPlane(ReadOnlySpan<V3d> polygon)
        {
            var n = polygon.Length;
            var centroid = V3d.Zero;
            for (var i = 0; i < n; i++) centroid += polygon[i];
            centroid /= n;

            var normal = V3d.Zero;
            var prev = polygon[n - 1] - centroid;
            for (var i = 0; i < n; i++)
            {
                var cur = polygon[i] - centroid;
                normal += prev.Cross(cur);
                prev = cur;
            }
            var len = normal.Length;
            if (len <= 0.0) return new Plane3d(V3d.Zero, 0.0); // degenerate marker
            normal /= len;
            return new Plane3d(normal, normal.Dot(centroid));
        }

        /// <summary>
        /// Projects a point into the 2D frame of a plane by dropping the
        /// dominant normal axis, with axis order chosen so that a polygon that
        /// is counter-clockwise around the normal stays counter-clockwise in 2D.
        /// </summary>
        public static V2d ProjectDominant(in V3d normal, in V3d p)
        {
            var ax = normal.X.Abs(); var ay = normal.Y.Abs(); var az = normal.Z.Abs();
            if (az >= ax && az >= ay) return normal.Z >= 0 ? new V2d(p.X, p.Y) : new V2d(p.Y, p.X);
            if (ax >= ay) return normal.X >= 0 ? new V2d(p.Y, p.Z) : new V2d(p.Z, p.Y);
            return normal.Y >= 0 ? new V2d(p.Z, p.X) : new V2d(p.X, p.Z);
        }

        /// <summary>
        /// Triangulates the polygon given by 2D vertices (in face winding
        /// order). Emits triangles as (i0,i1,i2) polygon-slot triples with the
        /// input winding. Returns false if no valid triangulation was found
        /// (self-intersecting or fully degenerate polygon).
        /// Slots of collinear (zero-area) ears are clipped silently.
        /// </summary>
        public static bool EarClip(ReadOnlySpan<V2d> p, Eps eps, List<(int I0, int I1, int I2)> triangles)
        {
            var n = p.Length;
            if (n < 3) return false;
            if (n == 3)
            {
                if (eps.AreaSign(p[0], p[1], p[2]) == Sign3.On) return true; // degenerate, emit nothing
                triangles.Add((0, 1, 2));
                return true;
            }

            // polygon orientation from the signed area sum
            var area2 = 0.0;
            for (int i = 0, j = n - 1; i < n; j = i++)
                area2 += p[j].X * p[i].Y - p[i].X * p[j].Y;
            var winding = area2 >= 0 ? Sign3.Above : Sign3.Below;

            // doubly linked list over active slots
            var next = new int[n];
            var prev = new int[n];
            for (var i = 0; i < n; i++) { next[i] = (i + 1) % n; prev[i] = (i + n - 1) % n; }

            var remaining = n;
            var cur = 0;
            var sinceLastClip = 0;
            while (remaining > 3)
            {
                var a = prev[cur]; var b = cur; var c = next[cur];
                var sign = eps.AreaSign(p[a], p[b], p[c]);

                bool clip;
                bool emit;
                if (sign == Sign3.On)
                {
                    // collinear corner: remove without emitting
                    clip = true; emit = false;
                }
                else if (sign == winding)
                {
                    // convex corner: valid ear iff no other active vertex lies
                    // inside (or on the boundary of) the candidate triangle
                    clip = true; emit = true;
                    for (var v = next[c]; v != a; v = next[v])
                    {
                        if (ContainsInclusive(p[a], p[b], p[c], p[v], winding, eps))
                        {
                            clip = false; emit = false;
                            break;
                        }
                    }
                }
                else
                {
                    clip = false; emit = false;
                }

                if (clip)
                {
                    if (emit) triangles.Add((a, b, c));
                    next[a] = c; prev[c] = a;
                    remaining--;
                    cur = a;
                    sinceLastClip = 0;
                }
                else
                {
                    cur = next[cur];
                    if (++sinceLastClip > remaining) return false; // full round without progress
                }
            }

            var f0 = cur; var f1 = next[cur]; var f2 = next[next[cur]];
            if (eps.AreaSign(p[f0], p[f1], p[f2]) != Sign3.On)
                triangles.Add((f0, f1, f2));
            return true;
        }

        /// <summary>
        /// True if q lies inside triangle (a,b,c) of the given winding, or on
        /// its boundary. Boundary counts as inside so that touching vertices
        /// conservatively block an ear.
        /// </summary>
        private static bool ContainsInclusive(
            in V2d a, in V2d b, in V2d c, in V2d q, Sign3 winding, Eps eps)
        {
            var opposite = winding == Sign3.Above ? Sign3.Below : Sign3.Above;
            if (eps.AreaSign(a, b, q) == opposite) return false;
            if (eps.AreaSign(b, c, q) == opposite) return false;
            if (eps.AreaSign(c, a, q) == opposite) return false;
            return true;
        }
    }
}
