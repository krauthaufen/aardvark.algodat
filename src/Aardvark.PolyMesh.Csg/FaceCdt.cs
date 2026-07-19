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
        private double m_aliasTol;
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
            // point-alias radius from area-predicate feasibility: a segment of
            // length s only admits a strict Area sign against a point at
            // height h when s*h > eps*(m+h+Scene)*h*factor, i.e. when
            // s > eps*(m+L+Scene)*factor — two points closer than that cannot
            // be separated by any predicate in this face and must unify
            var mag = Fun.Max(p0.NormMax, p1.NormMax, p2.NormMax);
            var ext = Fun.Max((p1 - p0).NormMax, (p2 - p0).NormMax, (p2 - p1).NormMax);
            m_aliasTol = 4 * m_eps.Relative * (mag + ext + m_eps.Scene) * m_factor;
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

        /// <summary>
        /// Inserts a point with raw-double routing: raw orientation always
        /// yields a valid subdivision, so no degeneracy healing is needed.
        /// Identity was decided globally — only bit-exact 2D corner
        /// duplicates alias (a pair coincident in this projection cannot be
        /// separated by any predicate here). Points eps-inside but raw-outside
        /// (guaranteed absorbable upstream) clamp onto the nearest boundary
        /// sub-edge.
        /// </summary>
        public int InsertPoint(int kernelId, V2d p)
        {
            if (m_localOfKernel.TryGetValue(kernelId, out var known)) return known;

            static double Det(in V2d a, in V2d b, in V2d c)
                => (b.X - a.X) * (c.Y - a.Y) - (b.Y - a.Y) * (c.X - a.X);

            for (var t = 0; t < m_t0.Count; t++)
            {
                if (m_dead[t]) continue;
                var (a, b, c) = Tri(t);
                var d0 = Det(m_pos[a], m_pos[b], p);
                var d1 = Det(m_pos[b], m_pos[c], p);
                var d2 = Det(m_pos[c], m_pos[a], p);
                if (d0 < 0 || d1 < 0 || d2 < 0) continue;
                var z0 = d0 == 0; var z1 = d1 == 0; var z2 = d2 == 0;
                var zeros = (z0 ? 1 : 0) + (z1 ? 1 : 0) + (z2 ? 1 : 0);
                if (zeros >= 2)
                {
                    var corner = z0 && z1 ? b : z1 && z2 ? c : a;
                    m_localOfKernel[kernelId] = corner;
                    return corner;
                }
                var li = AddPointRaw(kernelId, p);
                if (zeros == 1)
                {
                    var (u, v) = z0 ? (a, b) : z1 ? (b, c) : (c, a);
                    SplitEdgeRaw(u, v, li);
                }
                else
                {
                    m_dead[t] = true;
                    AddTri(a, b, li); AddTri(b, c, li); AddTri(c, a, li);
                }
                LegalizeAround(li);
                return li;
            }

            // raw-outside every triangle (upstream guaranteed eps-inside):
            // pull the point just inside the nearest boundary triangle —
            // splitting the boundary edge itself would create a subdivision
            // the neighbor face cannot see (a T-junction by construction)
            var bTri = -1; var bProj = V2d.Zero; var bd = double.MaxValue;
            for (var t = 0; t < m_t0.Count; t++)
            {
                if (m_dead[t]) continue;
                var (a, b, c) = Tri(t);
                Span<int> e = stackalloc int[] { a, b, b, c, c, a };
                for (var i = 0; i < 3; i++)
                {
                    var u = e[i * 2]; var v = e[i * 2 + 1];
                    if (FindTriWithEdge(v, u) >= 0) continue;
                    var pu = m_pos[u]; var pv = m_pos[v];
                    var dv = pv - pu;
                    var tt = (dv.Dot(p - pu) / dv.LengthSquared.Max(1e-300)).Clamp(1e-9, 1 - 1e-9);
                    var proj = pu + tt * dv;
                    var dist = (p - proj).LengthSquared;
                    if (dist < bd) { bd = dist; bTri = t; bProj = proj; }
                }
            }
            if (bTri < 0) throw new CsgVerificationException("point to insert lies outside the face");
            var (ba, bb, bc) = Tri(bTri);
            var centroid = (m_pos[ba] + m_pos[bb] + m_pos[bc]) / 3.0;
            var inside = bProj + 1e-9 * (centroid - bProj);
            var lo = AddPointRaw(kernelId, inside);
            m_dead[bTri] = true;
            AddTri(ba, bb, lo); AddTri(bb, bc, lo); AddTri(bc, ba, lo);
            LegalizeAround(lo);
            return lo;
        }

        /// <summary>
        /// Deterministic boundary subdivision: splits the current chain
        /// sub-edge (prev→end) of a face edge at a registered edge point.
        /// The 2D position is expected to lie exactly on the segment (lerped
        /// by the caller), so the split is raw-valid by construction — no
        /// band-based routing, no healing. Points at the exact position of
        /// the chain predecessor or the end corner alias to it (identical
        /// decision in every face sharing the edge).
        /// </summary>
        /// <summary>True when the kernel id already has a local vertex.</summary>
        public bool KnowsKernel(int kernelId) => m_localOfKernel.ContainsKey(kernelId);

        /// <summary>Maps kernelId to the local vertex of kernelTarget.</summary>
        public void AliasKernel(int kernelId, int kernelTarget)
        {
            if (!m_localOfKernel.ContainsKey(kernelId))
                m_localOfKernel[kernelId] = m_localOfKernel[kernelTarget];
        }

        public int InsertOnEdge(int prevKernel, int endKernel, int kernelId, V2d p)
        {
            if (m_localOfKernel.TryGetValue(kernelId, out var known)) return known;
            var a = m_localOfKernel[prevKernel];
            var b = m_localOfKernel[endKernel];
            if (p == m_pos[a]) { m_localOfKernel[kernelId] = a; return a; }
            if (p == m_pos[b]) { m_localOfKernel[kernelId] = b; return b; }
            if (FindTriWithEdge(a, b) < 0 && FindTriWithEdge(b, a) < 0)
                throw new CsgVerificationException("boundary chain edge missing during edge subdivision");
            var li = AddPointRaw(kernelId, p);
            SplitEdgeRaw(a, b, li);
            LegalizeAround(li);
            return li;
        }

        /// <summary>Splits edge (u,v) at li without degeneracy healing (caller guarantees validity).</summary>
        private void SplitEdgeRaw(int u, int v, int li)
        {
            var t = FindTriWithEdge(u, v);
            if (t >= 0)
            {
                var w = ThirdVertex(t, u, v);
                m_dead[t] = true;
                AddTri(u, li, w); AddTri(li, v, w);
            }
            var nt = FindTriWithEdge(v, u);
            if (nt >= 0)
            {
                var x = ThirdVertex(nt, v, u);
                m_dead[nt] = true;
                AddTri(v, li, x); AddTri(li, u, x);
            }
            if (m_constrained.Contains(Key(u, v)))
            {
                m_constrained.Remove(Key(u, v));
                m_constrained.Add(Key(u, li));
                m_constrained.Add(Key(li, v));
            }
        }


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
                // sub-band fallback: when every crossing test is On (micro
                // segments near the working resolution, or edges legalized
                // away), any triangulation inside the band is semantically
                // valid — recover the edge with raw-double signs, which is
                // pure topology repair and cannot contradict a committed
                // ternary decision
                if (!flipped) flipped = TryRawFlip(a, b, pa, pb);
                if (!flipped)
                {
                    var soup = "";
                    if (m_t0.Count < 40)
                        for (var t2 = 0; t2 < m_t0.Count; t2++)
                            if (!m_dead[t2]) soup += $" ({m_t0[t2]},{m_t1[t2]},{m_t2[t2]})";
                    throw new CsgVerificationException(
                        "constraint segment could not be recovered in face triangulation " +
                        $"(kernel {m_kernelIds[a]} local {a}@{m_pos[a]} -> kernel {m_kernelIds[b]} local {b}@{m_pos[b]}, " +
                        $"{m_pos.Count} points, factor {m_factor:0.#}, tris{soup})");
                }
            }
            throw new CsgVerificationException("constraint enforcement did not converge");
        }

        private bool TryRawFlip(int a, int b, in V2d pa, in V2d pb)
        {
            static double Det(in V2d p, in V2d q, in V2d r)
                => (q.X - p.X) * (r.Y - p.Y) - (q.Y - p.Y) * (r.X - p.X);
            for (var t = 0; t < m_t0.Count; t++)
            {
                if (m_dead[t]) continue;
                var (t0, t1, t2) = Tri(t);
                Span<int> e = stackalloc int[] { t0, t1, t1, t2, t2, t0 };
                for (var i = 0; i < 3; i++)
                {
                    var u = e[i * 2]; var v = e[i * 2 + 1];
                    if (u == a || u == b || v == a || v == b) continue;
                    if (m_constrained.Contains(Key(u, v))) continue;
                    var su = Det(pa, pb, m_pos[u]);
                    var sv = Det(pa, pb, m_pos[v]);
                    if (su == 0 || sv == 0 || (su > 0) == (sv > 0)) continue;
                    var sa2 = Det(m_pos[u], m_pos[v], pa);
                    var sb2 = Det(m_pos[u], m_pos[v], pb);
                    if (sa2 == 0 || sb2 == 0 || (sa2 > 0) == (sb2 > 0)) continue;
                    var ft = FindTriWithEdge(u, v);
                    var nt = FindTriWithEdge(v, u);
                    if (ft < 0 || nt < 0) continue;
                    var w = ThirdVertex(ft, u, v);
                    var x = ThirdVertex(nt, u, v);
                    if (Det(m_pos[u], m_pos[x], m_pos[w]) <= 0) continue;
                    if (Det(m_pos[x], m_pos[v], m_pos[w]) <= 0) continue;
                    m_dead[ft] = true; m_dead[nt] = true;
                    AddTri(u, x, w); AddTri(x, v, w);
                    return true;
                }
            }
            return false;
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
