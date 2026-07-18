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

            // directed edge map: (a,b) -> face; each undirected edge must occur
            // exactly once per direction
            var edges = new Dictionary<(int, int), int>(via.Length);
            for (var fi = 0; fi < faceCount; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                for (int i = start, j = end - 1; i < end; j = i++)
                {
                    var key = (via[j], via[i]);
                    if (edges.ContainsKey(key))
                        Err($"directed edge {key.Item1}->{key.Item2} occurs twice (non-manifold or inconsistent winding)");
                    else
                        edges[key] = fi;
                }
            }
            if (errors.Count > 0) return string.Join("; ", errors);

            foreach (var kvp in edges)
            {
                var (a, b) = kvp.Key;
                if (!edges.ContainsKey((b, a)))
                    Err($"edge {a}->{b} has no opposite (open surface or inconsistent winding)");
            }
            if (errors.Count > 0) return string.Join("; ", errors);

            // vertex-link check: the faces around each vertex must form one
            // closed cycle (rejects bowtie / pinch vertices)
            var vertexFaceDegree = new int[vertexCount];
            var outgoing = new Dictionary<int, (int To, int Face)>(via.Length); // one arbitrary outgoing edge per vertex
            var successor = new Dictionary<(int, int), int>(via.Length);        // (v, w) -> next vertex after v in the face containing edge (v, w)
            for (var fi = 0; fi < faceCount; fi++)
            {
                var start = fia[fi]; var end = fia[fi + 1];
                for (var i = start; i < end; i++)
                {
                    var v = via[i];
                    var w = via[i + 1 == end ? start : i + 1];
                    vertexFaceDegree[v]++;
                    outgoing[v] = (w, fi);
                    successor[(v, w)] = via[i + 2 >= end ? start + (i + 2 - end) : i + 2];
                }
            }
            for (var v = 0; v < vertexCount; v++)
            {
                if (vertexFaceDegree[v] == 0) continue; // unreferenced vertex is allowed
                // walk the fan: from edge (v,w) cross to the twin (w,v) and take
                // its successor at v, until we return to the start edge
                var startEdge = outgoing[v].To;
                var w = startEdge;
                var steps = 0;
                do
                {
                    w = successor[(w, v)];
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
