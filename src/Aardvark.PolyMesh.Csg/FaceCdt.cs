using System;
using System.Collections.Generic;
using System.Linq;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Small constrained triangulation of a single (projected) kernel triangle:
    /// the three CCW corners plus cut/boundary points, plus non-crossing
    /// constraint segments (the intersection curve restricted to this face).
    ///
    /// Incremental insertion with ternary orientation predicates; plain-double
    /// incircle is used only for quality (a "wrong" incircle sign still yields
    /// a valid triangulation, so it needs no eps treatment). Neighbor lookup is
    /// by linear scan — faces have few points, simplicity wins.
    ///
    /// Triangulate() verifies area coverage and constraint presence and throws
    /// CsgVerificationException on failure — never returns garbage.
    /// </summary>
    internal sealed class FaceCdt
    {
        private readonly Eps m_eps;
        private double m_factor = Eps.GenerationFactor;
        private readonly List<int> m_kernelIds = new();
        private readonly List<V2d> m_pos = new();
        private readonly Dictionary<int, int> m_localOfKernel = new();

        // triangle soup (parallel lists), dead triangles flagged
        private readonly List<int> m_t0 = new(), m_t1 = new(), m_t2 = new();
        private readonly List<bool> m_dead = new();

        private readonly HashSet<(int, int)> m_constrained = new();
        private readonly List<(int A, int B)> m_constraints = new();

        public FaceCdt(Eps eps, int k0, V2d p0, int k1, V2d p1, int k2, V2d p2)
        {
            m_eps = eps;
            Reset(k0, p0, k1, p1, k2, p2);
        }

        private FaceCdt(Eps eps) => m_eps = eps;

        [ThreadStatic]
        private static FaceCdt? s_pooled;

        /// <summary>Thread-local pooled instance (avoids ~10 collection allocations per cut face).</summary>
        public static FaceCdt Rent(Eps eps, double factor, int k0, V2d p0, int k1, V2d p1, int k2, V2d p2)
        {
            var cdt = s_pooled;
            if (cdt == null || !cdt.m_eps.Equals(eps)) s_pooled = cdt = new FaceCdt(eps);
            cdt.m_factor = factor;
            cdt.Reset(k0, p0, k1, p1, k2, p2);
            return cdt;
        }

        private void Reset(int k0, V2d p0, int k1, V2d p1, int k2, V2d p2)
        {
            m_kernelIds.Clear();
            m_pos.Clear();
            m_localOfKernel.Clear();
            m_t0.Clear(); m_t1.Clear(); m_t2.Clear();
            m_dead.Clear();
            m_constrained.Clear();
            m_constraints.Clear();
            m_healGuard = 0;
            AddPointRaw(k0, p0); AddPointRaw(k1, p1); AddPointRaw(k2, p2);
            if (Area(p0, p1, p2) != Sign3.Above)
                throw new CsgVerificationException("face is not counter-clockwise in its plane projection");
            AddTri(0, 1, 2);
        }

        private int AddPointRaw(int kernelId, V2d p)
        {
            var li = m_kernelIds.Count;
            m_kernelIds.Add(kernelId);
            m_pos.Add(p);
            m_localOfKernel[kernelId] = li;
            return li;
        }

        private void AddTri(int a, int b, int c) { m_t0.Add(a); m_t1.Add(b); m_t2.Add(c); m_dead.Add(false); }

        private (int V0, int V1, int V2) Tri(int t) => (m_t0[t], m_t1[t], m_t2[t]);

        /// <summary>
        /// All CDT predicates run at generation-1 tolerance: cut points carry
        /// one generation of derived error, and mixing tolerances between the
        /// pipeline (which classifies cuts at generation 1) and the CDT would
        /// let a point count as on-edge in one and outside in the other.
        /// </summary>
        private Sign3 Area(in V2d a, in V2d b, in V2d c) => m_eps.AreaSign(a, b, c, m_factor);

        private static (int, int) Key(int a, int b) => a < b ? (a, b) : (b, a);

        /// <summary>Inserts a point; returns its local id (an existing one if the point coincides with an existing vertex).</summary>
        public int InsertPoint(int kernelId, V2d p)
        {
            if (m_localOfKernel.TryGetValue(kernelId, out var known)) return known;

            for (var t = 0; t < m_t0.Count; t++)
            {
                if (m_dead[t]) continue;
                var (a, b, c) = Tri(t);
                var s0 = Area(m_pos[a], m_pos[b], p);
                var s1 = Area(m_pos[b], m_pos[c], p);
                var s2 = Area(m_pos[c], m_pos[a], p);
                if (s0 == Sign3.Below || s1 == Sign3.Below || s2 == Sign3.Below) continue;

                var onCount = (s0 == Sign3.On ? 1 : 0) + (s1 == Sign3.On ? 1 : 0) + (s2 == Sign3.On ? 1 : 0);
                if (onCount >= 2)
                {
                    // on two edge lines at once: either genuinely coincident
                    // with their shared corner (alias), or a sliver corner —
                    // then continue with the edge line the point is closer to
                    var corner = s0 == Sign3.On && s1 == Sign3.On ? b
                               : s1 == Sign3.On && s2 == Sign3.On ? c : a;
                    if (m_eps.AreCoincident(p, m_pos[corner], m_factor))
                    {
                        m_localOfKernel[kernelId] = corner;
                        return corner;
                    }
                    var d0 = s0 == Sign3.On ? LineDist2(m_pos[a], m_pos[b], p) : double.MaxValue;
                    var d1 = s1 == Sign3.On ? LineDist2(m_pos[b], m_pos[c], p) : double.MaxValue;
                    var d2 = s2 == Sign3.On ? LineDist2(m_pos[c], m_pos[a], p) : double.MaxValue;
                    s0 = d0 <= d1 && d0 <= d2 ? Sign3.On : Sign3.Above;
                    s1 = d1 < d0 && d1 <= d2 ? Sign3.On : Sign3.Above;
                    s2 = d2 < d0 && d2 < d1 ? Sign3.On : Sign3.Above;
                }

                var li = AddPointRaw(kernelId, p);
                if (s0 != Sign3.On && s1 != Sign3.On && s2 != Sign3.On)
                {
                    // interior: 1 -> 3
                    m_dead[t] = true;
                    AddTriChecked(a, b, li); AddTriChecked(b, c, li); AddTriChecked(c, a, li);
                }
                else
                {
                    // on one edge: split it in this triangle and in the neighbor (if any)
                    var (u, v) = s0 == Sign3.On ? (a, b) : s1 == Sign3.On ? (b, c) : (c, a);
                    SplitEdgeAt(u, v, li);
                }
                LegalizeAround(li);
                return li;
            }
            throw new CsgVerificationException("point to insert lies outside the face");
        }

        private static double LineDist2(in V2d a, in V2d b, in V2d p)
        {
            var d = b - a;
            var det = d.X * (p.Y - a.Y) - d.Y * (p.X - a.X);
            return det * det / d.LengthSquared.Max(1e-300);
        }

        /// <summary>Splits edge (u,v) at point li in both incident triangles, maintaining the constrained-edge set.</summary>
        private void SplitEdgeAt(int u, int v, int li)
        {
            var t = FindTriWithEdge(u, v);
            if (t >= 0)
            {
                var w = ThirdVertex(t, u, v);
                m_dead[t] = true;
                AddTriChecked(u, li, w); AddTriChecked(li, v, w);
            }
            var nt = FindTriWithEdge(v, u);
            if (nt >= 0)
            {
                var x = ThirdVertex(nt, v, u);
                m_dead[nt] = true;
                AddTriChecked(v, li, x); AddTriChecked(li, u, x);
            }
            if (m_constrained.Contains(Key(u, v)))
            {
                m_constrained.Remove(Key(u, v));
                m_constrained.Add(Key(u, li));
                m_constrained.Add(Key(li, v));
            }
        }

        /// <summary>
        /// Adds a triangle unless it is degenerate (collinear within eps): a
        /// sliver split can produce a zero-area flap whose middle vertex lies
        /// on another edge line; the flap is dropped and the split propagated
        /// across its long edge instead, which restores a valid subdivision.
        /// </summary>
        private void AddTriChecked(int a, int b, int c)
        {
            if (Area(m_pos[a], m_pos[b], m_pos[c]) != Sign3.On)
            {
                AddTri(a, b, c);
                return;
            }
            if (m_healGuard++ > 1000)
                throw new CsgVerificationException("degenerate-triangle healing did not converge");
            var ab = (m_pos[a] - m_pos[b]).LengthSquared;
            var bc = (m_pos[b] - m_pos[c]).LengthSquared;
            var ca = (m_pos[c] - m_pos[a]).LengthSquared;
            var (p, q, mid) = ab >= bc && ab >= ca ? (a, b, c) : bc >= ca ? (b, c, a) : (c, a, b);
            SplitEdgeAt(p, q, mid);
        }

        private int m_healGuard;

        private int FindTriWithEdge(int u, int v)
        {
            for (var t = 0; t < m_t0.Count; t++)
            {
                if (m_dead[t]) continue;
                var (a, b, c) = Tri(t);
                if ((a == u && b == v) || (b == u && c == v) || (c == u && a == v)) return t;
            }
            return -1;
        }

        private int ThirdVertex(int t, int u, int v)
        {
            var (a, b, c) = Tri(t);
            if (a != u && a != v) return a;
            if (b != u && b != v) return b;
            return c;
        }

        private static double InCircle(V2d a, V2d b, V2d c, V2d d)
        {
            var ax = a.X - d.X; var ay = a.Y - d.Y;
            var bx = b.X - d.X; var by = b.Y - d.Y;
            var cx = c.X - d.X; var cy = c.Y - d.Y;
            var a2 = ax * ax + ay * ay;
            var b2 = bx * bx + by * by;
            var c2 = cx * cx + cy * cy;
            return ax * (by * c2 - b2 * cy) - ay * (bx * c2 - b2 * cx) + a2 * (bx * cy - by * cx);
        }

        /// <summary>Lawson flips around a freshly inserted point.</summary>
        private void LegalizeAround(int li)
        {
            var pending = new Stack<(int U, int V)>();
            for (var t = 0; t < m_t0.Count; t++)
            {
                if (m_dead[t]) continue;
                var (a, b, c) = Tri(t);
                if (a == li) pending.Push((b, c));
                else if (b == li) pending.Push((c, a));
                else if (c == li) pending.Push((a, b));
            }
            var guard = 0;
            while (pending.Count > 0 && guard++ < 1000)
            {
                var (u, v) = pending.Pop();
                if (m_constrained.Contains(Key(u, v))) continue;
                var t = FindTriWithEdge(u, v);
                var nt = FindTriWithEdge(v, u);
                if (t < 0 || nt < 0) continue;
                var w = ThirdVertex(t, u, v);
                var x = ThirdVertex(nt, u, v);
                if (InCircle(m_pos[u], m_pos[v], m_pos[w], m_pos[x]) <= 0) continue;
                // flip only if the resulting triangles are strictly CCW
                if (Area(m_pos[u], m_pos[x], m_pos[w]) != Sign3.Above) continue;
                if (Area(m_pos[x], m_pos[v], m_pos[w]) != Sign3.Above) continue;
                m_dead[t] = true; m_dead[nt] = true;
                AddTri(u, x, w); AddTri(x, v, w);
                pending.Push((u, x)); pending.Push((x, v));
            }
        }

        /// <summary>Registers a constraint segment between two already-inserted points (kernel ids).</summary>
        public void AddConstraint(int kernelA, int kernelB)
        {
            var a = m_localOfKernel[kernelA];
            var b = m_localOfKernel[kernelB];
            if (a != b) m_constraints.Add((a, b));
        }

        private void EnforceConstraint(int a, int b)
        {
            // split at through-vertices first: points lying On the segment
            // strictly between its endpoints partition it into sub-segments
            var pa = m_pos[a]; var pb = m_pos[b];
            var d = pb - pa;
            var len2 = d.LengthSquared;
            var through = new List<(double T, int V)>();
            for (var v = 0; v < m_pos.Count; v++)
            {
                if (v == a || v == b) continue;
                if (Area(pa, pb, m_pos[v]) != Sign3.On) continue;
                var t = d.Dot(m_pos[v] - pa);
                if (t <= 0 || t >= len2) continue;
                through.Add((t, v));
            }
            through.Sort((x, y) => x.T.CompareTo(y.T));

            var prev = a;
            foreach (var (_, v) in through)
            {
                EnforceSegment(prev, v);
                prev = v;
            }
            EnforceSegment(prev, b);
        }

        /// <summary>
        /// Makes edge (a,b) present by flipping crossing edges (Sloan-style
        /// constraint recovery); assumes no vertex lies On the open segment.
        /// </summary>
        private void EnforceSegment(int a, int b)
        {
            if (a == b) return;
            var pa = m_pos[a]; var pb = m_pos[b];
            var guard = 0;
            while (guard++ < 10000)
            {
                if (FindTriWithEdge(a, b) >= 0 || FindTriWithEdge(b, a) >= 0)
                {
                    m_constrained.Add(Key(a, b));
                    return;
                }

                var flipped = false;
                for (var t = 0; t < m_t0.Count && !flipped; t++)
                {
                    if (m_dead[t]) continue;
                    var (t0, t1, t2) = Tri(t);
                    Span<int> e = stackalloc int[] { t0, t1, t1, t2, t2, t0 };
                    for (var i = 0; i < 3 && !flipped; i++)
                    {
                        var u = e[i * 2]; var v = e[i * 2 + 1];
                        if (u == a || u == b || v == a || v == b) continue;
                        // does edge (u,v) cross the open segment a-b?
                        var su = Area(pa, pb, m_pos[u]);
                        var sv = Area(pa, pb, m_pos[v]);
                        if (!((su == Sign3.Above && sv == Sign3.Below) || (su == Sign3.Below && sv == Sign3.Above))) continue;
                        var sa2 = Area(m_pos[u], m_pos[v], pa);
                        var sb2 = Area(m_pos[u], m_pos[v], pb);
                        if (!((sa2 == Sign3.Above && sb2 == Sign3.Below) || (sa2 == Sign3.Below && sb2 == Sign3.Above))) continue;
                        if (m_constrained.Contains(Key(u, v)))
                            throw new CsgVerificationException("constraint segments cross each other in a face");

                        var ft = FindTriWithEdge(u, v);
                        var nt = FindTriWithEdge(v, u);
                        if (ft < 0 || nt < 0)
                            throw new CsgVerificationException("constraint crossing edge has no twin (left the face)");
                        var w = ThirdVertex(ft, u, v);
                        var x = ThirdVertex(nt, u, v);
                        // flip only when the quad is strictly convex
                        if (Area(m_pos[u], m_pos[x], m_pos[w]) != Sign3.Above) continue;
                        if (Area(m_pos[x], m_pos[v], m_pos[w]) != Sign3.Above) continue;
                        m_dead[ft] = true; m_dead[nt] = true;
                        AddTri(u, x, w); AddTri(x, v, w);
                        flipped = true;
                    }
                }
                if (!flipped)
                    throw new CsgVerificationException("constraint segment could not be recovered in face triangulation");
            }
            throw new CsgVerificationException("constraint enforcement did not converge");
        }

        /// <summary>
        /// Enforces all constraints and returns the triangulation as kernel-id
        /// triples (CCW) plus all constrained sub-edges as kernel-id pairs.
        /// Verifies area coverage before returning.
        /// </summary>
        public (List<(int, int, int)> Triangles, List<(int, int)> ConstraintEdges) Triangulate()
        {
            foreach (var (a, b) in m_constraints) EnforceConstraint(a, b);

            var tris = new List<(int, int, int)>();
            var area = 0.0;
            for (var t = 0; t < m_t0.Count; t++)
            {
                if (m_dead[t]) continue;
                var (a, b, c) = Tri(t);
                area += 0.5 * ((m_pos[b].X - m_pos[a].X) * (m_pos[c].Y - m_pos[a].Y)
                             - (m_pos[b].Y - m_pos[a].Y) * (m_pos[c].X - m_pos[a].X));
                tris.Add((m_kernelIds[a], m_kernelIds[b], m_kernelIds[c]));
            }
            var faceArea = 0.5 * ((m_pos[1].X - m_pos[0].X) * (m_pos[2].Y - m_pos[0].Y)
                                - (m_pos[1].Y - m_pos[0].Y) * (m_pos[2].X - m_pos[0].X));
            // healed degenerate slivers may each carry up to one widened
            // area-tolerance of the face factor
            var l = Fun.Max((m_pos[1] - m_pos[0]).NormMax, (m_pos[2] - m_pos[0]).NormMax, (m_pos[2] - m_pos[1]).NormMax);
            var m = Fun.Max(m_pos[0].NormMax, m_pos[1].NormMax, m_pos[2].NormMax);
            var sliverAllowance = m_factor * m_eps.Relative * (m + l + m_eps.Scene) * l * (m_pos.Count + 1);
            if ((area - faceArea).Abs() > 1e-6 * faceArea.Abs() + sliverAllowance + m_eps.Relative)
                throw new CsgVerificationException(
                    $"face triangulation does not cover the face (area {area} vs {faceArea})");

            var constraintEdges = m_constrained
                .Select(e => (m_kernelIds[e.Item1], m_kernelIds[e.Item2]))
                .ToList();
            return (tris, constraintEdges);
        }
    }
}
