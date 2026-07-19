using System;
using System.Collections.Generic;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Bounding-box tree over primitive boxes with dual-tree and ray
    /// traversals, using Aardvark.Base.BbTree's compressed combined-box node
    /// format (Box3dAndFlags): each inner node stores, per box side, the bound
    /// of whichever child does NOT achieve the parent union's bound plus a
    /// flag naming that child; child boxes are reconstructed by carrying the
    /// parent box down the descent.
    ///
    /// The builder is a Morton-order median split (one sort, O(n) nodes) —
    /// BbTree's SAH builder produces slightly better trees but costs three
    /// sorts per node, which dominated whole-pipeline profiles.
    ///
    /// NOTE (migration): builder and traversals are BbTree-format-compatible
    /// and intended to move to Aardvark.Base (as a fast-build option plus the
    /// missing queries).
    /// </summary>
    internal sealed class CsgBvh
    {
        public readonly int Count;
        public readonly Box3d[] Leaves;
        private readonly int[] m_left = Array.Empty<int>();   // node refs: >= 0 inner node, < 0 leaf (-1 - prim)
        private readonly int[] m_right = Array.Empty<int>();
        private readonly Box3dAndFlags[] m_combined = Array.Empty<Box3dAndFlags>();
        private readonly int m_root;
        private readonly Box3d m_rootBox;
        private int m_nodeCount;

        /// <summary>
        /// presorted: skip the Morton sort and median-split over the given
        /// order directly — for boxes that are already spatially coherent
        /// (e.g. boolean results, whose triangles are emitted grouped by
        /// parent face in source order), this makes the build O(n).
        /// </summary>
        public CsgBvh(Box3d[] leafBoxes, bool presorted = false)
        {
            Count = leafBoxes.Length;
            Leaves = leafBoxes;
            if (Count == 0) { m_rootBox = Box3d.Invalid; m_root = 0; return; }
            if (Count == 1) { m_rootBox = leafBoxes[0]; m_root = -1; return; }

            var order = new int[Count].SetByIndex(i => i);
            if (!presorted)
            {
                var bounds = new Box3d(leafBoxes);
                var keys = new uint[Count];
                var scale = new V3d(1023.0, 1023.0, 1023.0) / (bounds.Size + new V3d(1e-300));
                for (var i = 0; i < Count; i++)
                {
                    var c = (leafBoxes[i].Center - bounds.Min) * scale;
                    keys[i] = Morton((uint)c.X.Clamp(0, 1023), (uint)c.Y.Clamp(0, 1023), (uint)c.Z.Clamp(0, 1023));
                }
                Array.Sort(keys, order);
            }

            m_left = new int[Count - 1];
            m_right = new int[Count - 1];
            m_combined = new Box3dAndFlags[Count - 1];
            m_root = Build(order, 0, Count, out m_rootBox);
        }

        private static uint Morton(uint x, uint y, uint z)
            => (Spread(x) << 2) | (Spread(y) << 1) | Spread(z);

        private static uint Spread(uint v)
        {
            v = (v | (v << 16)) & 0x030000FF;
            v = (v | (v << 8)) & 0x0300F00F;
            v = (v | (v << 4)) & 0x030C30C3;
            v = (v | (v << 2)) & 0x09249249;
            return v;
        }

        private int Build(int[] order, int lo, int hi, out Box3d box)
        {
            if (hi - lo == 1)
            {
                box = Leaves[order[lo]];
                return -1 - order[lo];
            }
            var mid = (lo + hi) / 2;
            var l = Build(order, lo, mid, out var boxL);
            var r = Build(order, mid, hi, out var boxR);
            var ni = m_nodeCount++;
            box = Box.Union(boxL, boxR);
            m_left[ni] = l;
            m_right[ni] = r;
            m_combined[ni] = new Box3dAndFlags(box, boxL, boxR);
            return ni;
        }

        public Box3d RootBox => m_rootBox;

        private static Box3d Child0(in Box3dAndFlags c, in Box3d parent) => new(
            new V3d((c.BFlags & Box.Flags.MinX0) != 0 ? c.BBox.Min.X : parent.Min.X,
                    (c.BFlags & Box.Flags.MinY0) != 0 ? c.BBox.Min.Y : parent.Min.Y,
                    (c.BFlags & Box.Flags.MinZ0) != 0 ? c.BBox.Min.Z : parent.Min.Z),
            new V3d((c.BFlags & Box.Flags.MaxX0) != 0 ? c.BBox.Max.X : parent.Max.X,
                    (c.BFlags & Box.Flags.MaxY0) != 0 ? c.BBox.Max.Y : parent.Max.Y,
                    (c.BFlags & Box.Flags.MaxZ0) != 0 ? c.BBox.Max.Z : parent.Max.Z));

        private static Box3d Child1(in Box3dAndFlags c, in Box3d parent) => new(
            new V3d((c.BFlags & Box.Flags.MinX1) != 0 ? c.BBox.Min.X : parent.Min.X,
                    (c.BFlags & Box.Flags.MinY1) != 0 ? c.BBox.Min.Y : parent.Min.Y,
                    (c.BFlags & Box.Flags.MinZ1) != 0 ? c.BBox.Min.Z : parent.Min.Z),
            new V3d((c.BFlags & Box.Flags.MaxX1) != 0 ? c.BBox.Max.X : parent.Max.X,
                    (c.BFlags & Box.Flags.MaxY1) != 0 ? c.BBox.Max.Y : parent.Max.Y,
                    (c.BFlags & Box.Flags.MaxZ1) != 0 ? c.BBox.Max.Z : parent.Max.Z));

        /// <summary>
        /// Enumerates primitives whose leaf boxes are hit by the ray from o
        /// along dir (t >= 0), mapped through primToUser.
        /// </summary>
        public IEnumerable<int> RayCandidates(V3d o, V3d dir, int[] primToUser)
        {
            if (Count == 0) yield break;
            var inv = new V3d(1.0 / dir.X, 1.0 / dir.Y, 1.0 / dir.Z);
            var stack = new Stack<(int Ref, Box3d Box)>();
            stack.Push((m_root, m_rootBox));
            while (stack.Count > 0)
            {
                var (r, box) = stack.Pop();
                if (!RayHitsBox(o, inv, box)) continue;
                if (r < 0)
                {
                    yield return primToUser[-1 - r];
                }
                else
                {
                    var c = m_combined[r];
                    stack.Push((m_left[r], Child0(c, box)));
                    stack.Push((m_right[r], Child1(c, box)));
                }
            }
        }

        private static bool RayHitsBox(in V3d o, in V3d inv, in Box3d b)
        {
            var t0x = (b.Min.X - o.X) * inv.X; var t1x = (b.Max.X - o.X) * inv.X;
            var t0y = (b.Min.Y - o.Y) * inv.Y; var t1y = (b.Max.Y - o.Y) * inv.Y;
            var t0z = (b.Min.Z - o.Z) * inv.Z; var t1z = (b.Max.Z - o.Z) * inv.Z;
            var tMin = Fun.Max(t0x.Min(t1x), t0y.Min(t1y), t0z.Min(t1z)).Max(0.0);
            var tMax = Fun.Min(t0x.Max(t1x), t0y.Max(t1y), t0z.Max(t1z));
            return tMax >= tMin;
        }

        /// <summary>Reports every primitive pair (i in this, j in other) whose leaf boxes intersect.</summary>
        public void ForEachIntersectingPair(CsgBvh other, Action<int, int> emit)
        {
            if (Count == 0 || other.Count == 0) return;

            var stack = new Stack<(int RefA, Box3d BoxA, int RefB, Box3d BoxB)>();
            stack.Push((m_root, m_rootBox, other.m_root, other.m_rootBox));

            while (stack.Count > 0)
            {
                var (ra, ba, rb, bb) = stack.Pop();
                if (!ba.Intersects(bb)) continue;

                var leafA = ra < 0;
                var leafB = rb < 0;
                if (leafA && leafB)
                {
                    emit(-1 - ra, -1 - rb);
                }
                else if (leafA || (!leafB && bb.Volume > ba.Volume))
                {
                    var c = other.m_combined[rb];
                    stack.Push((ra, ba, other.m_left[rb], Child0(c, bb)));
                    stack.Push((ra, ba, other.m_right[rb], Child1(c, bb)));
                }
                else
                {
                    var c = m_combined[ra];
                    stack.Push((m_left[ra], Child0(c, ba), rb, bb));
                    stack.Push((m_right[ra], Child1(c, ba), rb, bb));
                }
            }
        }
    }
}
