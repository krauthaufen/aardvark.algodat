using System;
using System.Collections.Generic;
using Aardvark.Base;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Structural verification of indexed face sets: watertight orientable
    /// 2-manifold (every undirected edge has exactly one forward and one
    /// backward directed edge, every vertex link is a single closed fan).
    /// Used both to enforce the input contract and to verify output before it
    /// is handed to the user. Purely combinatorial — no geometry, no eps.
    /// </summary>
    /// <summary>
    /// Strong-mix hash for packed-long keys: long.GetHashCode() is hi^lo,
    /// which collides catastrophically for packed (a,b) index pairs of
    /// structured meshes and degenerates dictionaries to O(n) per op.
    /// </summary>
    internal sealed class MixedLongComparer : IEqualityComparer<long>
    {
        public static readonly MixedLongComparer Instance = new();
        public bool Equals(long x, long y) => x == y;
        public int GetHashCode(long x)
        {
            unchecked
            {
                var z = (ulong)x + 0x9E3779B97F4A7C15UL;
                z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
                z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
                return (int)(z ^ (z >> 31));
            }
        }
    }

    internal static class ManifoldChecks
    {
        /// <summary>
        /// Checks that the face set is a closed orientable 2-manifold.
        /// Returns null on success, otherwise a description of the first
        /// problems found (at most a handful, for exception messages).
        /// </summary>
        public static string? FindManifoldViolation(int[] fia, int[] via, int vertexCount)
        {
            var faceCount = fia.Length - 1;
            if (via.Length == faceCount * 3 && (faceCount == 0 || fia[1] == 3))
                return FindManifoldViolationTriangles(via, vertexCount);
            var errors = new List<string>();
            void Err(string e) { if (errors.Count < 8) errors.Add(e); }

            // per-face sanity: index ranges, degree >= 3, no repeated vertices
            for (var fi = 0; fi < faceCount && errors.Count < 8; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                var fvc = end - start;
                if (fvc < 3) { Err($"face {fi} has only {fvc} vertices"); continue; }
                for (var i = start; i < end; i++)
                {
                    var vi = via[i];
                    if (vi < 0 || vi >= vertexCount) { Err($"face {fi} references invalid vertex {vi}"); break; }
                    for (var j = start; j < i; j++)
                        if (via[j] == vi) { Err($"face {fi} repeats vertex {vi}"); j = i = end; }
                }
            }
            if (errors.Count > 0) return string.Join("; ", errors);

            // directed edge map (long-keyed): each undirected edge must occur
            // exactly once per direction
            static long DKey(int a, int b) => ((long)a << 32) | (uint)b;
            var edges = new Dictionary<long, int>(via.Length, MixedLongComparer.Instance);
            for (var fi = 0; fi < faceCount; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                for (int i = start, j = end - 1; i < end; j = i++)
                {
                    var key = DKey(via[j], via[i]);
                    if (edges.ContainsKey(key))
                        Err($"directed edge {via[j]}->{via[i]} occurs twice (non-manifold or inconsistent winding)");
                    else
                        edges[key] = fi;
                }
            }
            if (errors.Count > 0) return string.Join("; ", errors);

            foreach (var kvp in edges)
            {
                var a = (int)(kvp.Key >> 32); var b = (int)kvp.Key;
                if (!edges.ContainsKey(DKey(b, a)))
                    Err($"edge {a}->{b} has no opposite (open surface or inconsistent winding)");
            }
            if (errors.Count > 0) return string.Join("; ", errors);

            // vertex-link check: the faces around each vertex must form one
            // closed cycle (rejects bowtie / pinch vertices)
            var vertexFaceDegree = new int[vertexCount];
            var outgoing = new int[vertexCount];
            var successor = new Dictionary<long, int>(via.Length, MixedLongComparer.Instance); // (v, w) -> next vertex after v in the face containing edge (v, w)
            for (var fi = 0; fi < faceCount; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                for (var i = start; i < end; i++)
                {
                    var v = via[i];
                    var w = via[i + 1 == end ? start : i + 1];
                    vertexFaceDegree[v]++;
                    outgoing[v] = w;
                    successor[DKey(v, w)] = via[i + 2 >= end ? start + (i + 2 - end) : i + 2];
                }
            }
            for (var v = 0; v < vertexCount; v++)
            {
                if (vertexFaceDegree[v] == 0) continue; // unreferenced vertex is allowed
                // walk the fan: from edge (v,w) cross to the twin (w,v) and take
                // its successor at v, until we return to the start edge
                var startEdge = outgoing[v];
                var w = startEdge;
                var steps = 0;
                do
                {
                    w = successor[DKey(w, v)];
                    if (++steps > vertexFaceDegree[v]) break;
                }
                while (w != startEdge);
                if (steps != vertexFaceDegree[v])
                {
                    Err($"vertex {v} has a disconnected link ({vertexFaceDegree[v]} incident faces, fan of {steps})");
                    if (errors.Count >= 8) break;
                }
            }
            return errors.Count > 0 ? string.Join("; ", errors) : null;
        }

        /// <summary>
        /// Triangle fast path: twin matching via one sort instead of
        /// dictionaries (hot: runs on every boolean output component).
        /// </summary>
        private static string? FindManifoldViolationTriangles(int[] via, int vertexCount)
        {
            var h = via.Length; // halfedge count
            for (var i = 0; i < h; i++)
                if ((uint)via[i] >= (uint)vertexCount)
                    return $"face {i / 3} references invalid vertex {via[i]}";
            for (var t = 0; t < h; t += 3)
                if (via[t] == via[t + 1] || via[t + 1] == via[t + 2] || via[t + 2] == via[t])
                    return $"face {t / 3} repeats a vertex";

            // one sort by UNDIRECTED key: each edge's two halfedges become an
            // adjacent pair in the sorted order — twins with no second search
            static long UKey(int a, int b) => a < b ? ((long)a << 32) | (uint)b : ((long)b << 32) | (uint)a;
            var keys = new long[h];
            var ids = new int[h];
            for (var t = 0; t < h; t += 3)
            {
                keys[t] = UKey(via[t], via[t + 1]); ids[t] = t;
                keys[t + 1] = UKey(via[t + 1], via[t + 2]); ids[t + 1] = t + 1;
                keys[t + 2] = UKey(via[t + 2], via[t]); ids[t + 2] = t + 2;
            }
            RadixSorter.SortEdgeKeys(keys, ids, h);

            var twin = new int[h];
            for (var i = 0; i < h;)
            {
                var j = i + 1;
                while (j < h && keys[j] == keys[i]) j++;
                if (j - i != 2)
                    return j - i == 1
                        ? $"edge {(int)(keys[i] >> 32)}-{(int)keys[i]} has no opposite (open surface or inconsistent winding)"
                        : $"edge {(int)(keys[i] >> 32)}-{(int)keys[i]} has {j - i} incident faces (non-manifold)";
                var h0 = ids[i]; var h1 = ids[i + 1];
                if (via[h0] == via[h1])
                    return $"directed edge {via[h0]}->{via[h0 - h0 % 3 + (h0 + 1) % 3]} occurs twice (non-manifold or inconsistent winding)";
                twin[h0] = h1;
                twin[h1] = h0;
                i = j;
            }

            // vertex links: walk the fan around each vertex through twins;
            // it must visit every incident halfedge (rejects pinch vertices)
            var degree = new int[vertexCount];
            var outgoing = new int[vertexCount];
            for (var i = 0; i < h; i++)
            {
                var v = via[i];
                degree[v]++;
                outgoing[v] = i;
            }
            for (var v = 0; v < vertexCount; v++)
            {
                if (degree[v] == 0) continue;
                var start = outgoing[v];
                var e = start;
                var steps = 0;
                do
                {
                    // previous halfedge in the face of e, then across
                    var prev = e - e % 3 + (e + 2) % 3;
                    e = twin[prev];
                    if (++steps > degree[v]) break;
                }
                while (e != start);
                if (steps != degree[v])
                    return $"vertex {v} has a disconnected link ({degree[v]} incident faces, fan of {steps})";
            }
            return null;
        }

        /// <summary>
        /// Partitions faces into edge-connected components. Returns the number
        /// of components and fills componentOfFace (length faceCount).
        /// Assumes the face set already passed the manifold check.
        /// </summary>
        public static int EdgeConnectedComponents(int[] fia, int[] via, int[] componentOfFace)
        {
            var faceCount = fia.Length - 1;
            var edges = new Dictionary<(int, int), int>(via.Length);
            for (var fi = 0; fi < faceCount; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                for (int i = start, j = end - 1; i < end; j = i++)
                    edges[(via[j], via[i])] = fi;
            }

            componentOfFace.Set(-1);
            var componentCount = 0;
            var stack = new Stack<int>();
            for (var seed = 0; seed < faceCount; seed++)
            {
                if (componentOfFace[seed] >= 0) continue;
                var ci = componentCount++;
                stack.Push(seed);
                componentOfFace[seed] = ci;
                while (stack.Count > 0)
                {
                    var fi = stack.Pop();
                    var start = fia[fi]; var end = fia[fi + 1];
                    for (int i = start, j = end - 1; i < end; j = i++)
                    {
                        if (edges.TryGetValue((via[i], via[j]), out var nf) && componentOfFace[nf] < 0)
                        {
                            componentOfFace[nf] = ci;
                            stack.Push(nf);
                        }
                    }
                }
            }
            return componentCount;
        }
    }
}
