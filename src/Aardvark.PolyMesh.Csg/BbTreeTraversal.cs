using System;
using System.Collections.Generic;
using System.Reflection;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Dual-tree traversal over Aardvark.Base.BbTree using the compressed
    /// combined box array (Box3dAndFlags): each inner node stores, per box
    /// side, the bound of whichever child does NOT achieve the parent union's
    /// bound plus a flag naming that child; child boxes are reconstructed by
    /// carrying the parent box down the descent.
    ///
    /// NOTE (migration): BbTree.m_combinedBoxArray is currently private, so it
    /// is fetched via reflection once per tree. When these traversals move to
    /// Aardvark.Base, add a public accessor instead.
    /// </summary>
    internal sealed class CsgBvh
    {
        private static readonly FieldInfo s_combinedField = typeof(BbTree)
            .GetField("m_combinedBoxArray", BindingFlags.NonPublic | BindingFlags.Instance)!;

        public readonly int Count;
        public readonly Box3d[] Leaves;         // per-primitive boxes (pre-enlarged by the caller)
        public readonly BbTree? Tree;           // null for Count < 2
        public readonly Box3dAndFlags[]? Combined;

        public CsgBvh(Box3d[] leafBoxes)
        {
            Count = leafBoxes.Length;
            Leaves = leafBoxes;
            if (Count > 1)
            {
                Tree = new BbTree(leafBoxes, BbTree.BuildFlags.CreateCombinedArray | BbTree.BuildFlags.LeafLimit01);
                Combined = (Box3dAndFlags[])s_combinedField.GetValue(Tree)!;
            }
        }

        public Box3d RootBox => Count == 0 ? Box3d.Invalid : Count == 1 ? Leaves[0] : Tree!.Box3d;

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
        /// Reports every primitive pair (i in this, j in other) whose leaf
        /// boxes intersect. Leaf boxes should be pre-enlarged with the eps
        /// slack by the caller.
        /// </summary>
        public void ForEachIntersectingPair(CsgBvh other, Action<int, int> emit)
        {
            if (Count == 0 || other.Count == 0) return;

            // node refs: >= 0 inner node index, < 0 leaf primitive (-1 - prim)
            var stack = new Stack<(int RefA, Box3d BoxA, int RefB, Box3d BoxB)>();
            var rootA = Count == 1 ? -1 : 0;
            var rootB = other.Count == 1 ? -1 : 0;
            stack.Push((rootA, RootBox, rootB, other.RootBox));

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
                    // descend B
                    var c = other.Combined![rb];
                    stack.Push((ra, ba, other.Tree!.GetLeft(rb), Child0(c, bb)));
                    stack.Push((ra, ba, other.Tree!.GetRight(rb), Child1(c, bb)));
                }
                else
                {
                    // descend A
                    var c = Combined![ra];
                    stack.Push((Tree!.GetLeft(ra), Child0(c, ba), rb, bb));
                    stack.Push((Tree!.GetRight(ra), Child1(c, ba), rb, bb));
                }
            }
        }
    }
}
