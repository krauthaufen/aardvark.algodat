﻿/*
    Copyright (C) 2006-2018. Aardvark Platform Team. http://github.com/aardvark-platform.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Aardvark.Base;
using Aardvark.Data;
using Aardvark.Data.Points;

namespace Aardvark.Geometry.Points
{
    /// <summary>
    /// </summary>
    public static class DeleteExtensions
    {
        /// <summary>
        /// Returns new pointset with all points deleted which are inside.
        /// </summary>
        public static PointSet Delete(this PointSet node,
            Func<PointSetNode, bool> isNodeFullyInside,
            Func<PointSetNode, bool> isNodeFullyOutside,
            Func<V3d, bool> isPositionInside,
            Storage storage, CancellationToken ct
            )
        {
            var root = Delete((PointSetNode)node.Octree.Value, isNodeFullyInside, isNodeFullyOutside, isPositionInside, storage, ct);
            var newId = Guid.NewGuid().ToString();
            var result = new PointSet(node.Storage, newId, root?.Id, node.SplitLimit);
            node.Storage.Add(newId, result);
            return result;
        }

        /// <summary>
        /// </summary>
        public static PointSetNode Delete(this PointSetNode node,
            Func<PointSetNode, bool> isNodeFullyInside,
            Func<PointSetNode, bool> isNodeFullyOutside,
            Func<V3d, bool> isPositionInside,
            Storage storage, CancellationToken ct
            )
        {
            if (node == null) return null;
            if (isNodeFullyInside(node)) return null;
            if (isNodeFullyOutside(node)) return node;

            Guid? newPsId = null;
            Guid? newCsId = null;
            Guid? newNsId = null;
            Guid? newIsId = null;
            Guid? newKsId = null;
            Guid? newKdId = null;
            
            var ps = node.HasPositions ? new List<V3f>() : null;
            var cs = node.HasColors ? new List<C4b>() : null;
            var ns = node.HasNormals ? new List<V3f>() : null;
            var js = node.HasIntensities ? new List<int>() : null;
            var ks = node.HasClassifications ? new List<byte>() : null;
            var oldPsAbsolute = node.PositionsAbsolute;
            var oldPs = node.Positions?.Value;
            var oldCs = node.Colors?.Value;
            var oldNs = node.Normals?.Value;
            var oldIs = node.Intensities?.Value;
            var oldKs = node.Classifications?.Value;
            for (var i = 0; i < oldPsAbsolute.Length; i++)
            {
                if (!isPositionInside(oldPsAbsolute[i]))
                {
                    if (oldPs != null) ps.Add(oldPs[i]);
                    if (oldCs != null) cs.Add(oldCs[i]);
                    if (oldNs != null) ns.Add(oldNs[i]);
                    if (oldIs != null) js.Add(oldIs[i]);
                    if (oldKs != null) ks.Add(oldKs[i]);
                }
            }

            var data = ImmutableDictionary<Durable.Def, object>.Empty
                .Add(Durable.Octree.NodeId, Guid.NewGuid())
                .Add(Durable.Octree.Cell, node.Cell)
                ;

            if (node.HasPositions)
            {
                newPsId = Guid.NewGuid();
                var psa = ps.ToArray();
                storage.Add(newPsId.Value, psa);

                newKdId = Guid.NewGuid();
                storage.Add(newKdId.Value, psa.Length != 0 ? psa.BuildKdTree().Data : new PointRkdTreeFData());

                data = data
                    .Add(Durable.Octree.PositionsLocal3fReference, newPsId)
                    .Add(Durable.Octree.PointRkdTreeFDataReference, newKdId)
                    ;
            }

            if (node.HasColors)
            {
                newCsId = Guid.NewGuid();
                storage.Add(newCsId.Value, cs.ToArray());

                data = data.Add(Durable.Octree.Colors4bReference, newCsId);
            }

            if (node.HasNormals)
            {
                newNsId = Guid.NewGuid();
                storage.Add(newNsId.Value, ns.ToArray());

                data = data.Add(Durable.Octree.Normals3fReference, newNsId);
            }

            if (node.HasIntensities)
            {
                newIsId = Guid.NewGuid();
                storage.Add(newIsId.Value, js.ToArray());

                data = data.Add(Durable.Octree.Intensities1iReference, newIsId);
            }

            if (node.HasClassifications)
            {
                newKsId = Guid.NewGuid();
                storage.Add(newKsId.Value, ks.ToArray());

                data = data.Add(Durable.Octree.Classifications1bReference, newKsId);
            }

            var newSubnodes = node.Subnodes?.Map(n => n?.Value.Delete(isNodeFullyInside, isNodeFullyOutside, isPositionInside, storage, ct));
            if (newSubnodes != null && newSubnodes.All(n => n == null)) newSubnodes = null;
            if (ps.Count == 0 && newSubnodes == null) return null;

            var pointCountTreeLeafs = newSubnodes != null ? (newSubnodes.Sum(n => n != null ? n.PointCountTree : 0)) : ps.Count;
            data = data.Add(Durable.Octree.PointCountTreeLeafs, pointCountTreeLeafs);

            if (newSubnodes != null)
            {
                var newSubnodesIds = newSubnodes.Map(x => x?.Id ?? Guid.Empty);
                data = data.Add(Durable.Octree.SubnodesGuids, newSubnodesIds);
            }

            var result = new PointSetNode(data, storage, writeToStore: true);
            return result;
        }
    }
}
