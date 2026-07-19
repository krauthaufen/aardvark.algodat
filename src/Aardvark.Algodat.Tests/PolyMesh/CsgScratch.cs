using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>Manual tools: big fuzz sweep and perf measurement (Explicit).</summary>
    [TestFixture]
    public class CsgScratch
    {
        [Test]
        [Explicit]
        public void BigFuzz()
        {
            var fails = 0; var total = 0;
            void Check(PolyMesh a, PolyMesh b, string ctx)
            {
                total++;
                try
                {
                    var va = CsgM0Tests.Volume(a); var vb = CsgM0Tests.Volume(b);
                    var vu = Csg.Union(a, b).Sum(CsgM0Tests.Volume);
                    var vi = Csg.Intersection(a, b).Sum(CsgM0Tests.Volume);
                    var vd = Csg.Difference(a, b).Sum(CsgM0Tests.Volume);
                    var scale = va.Abs() + vb.Abs();
                    if ((vu + vi - va - vb).Abs() > 1e-9 * scale) throw new Exception($"volume identity union: {vu}+{vi} != {va}+{vb}");
                    if ((vd + vi - va).Abs() > 1e-9 * scale) throw new Exception($"volume identity diff: {vd}+{vi} != {va}");
                }
                catch (Exception e)
                {
                    fails++;
                    Console.WriteLine($"FAIL {ctx}: {e.Message}");
                }
            }

            var rnd = new RandomSystem(123456);
            for (var i = 0; i < 1500; i++)
            {
                var t = new V3d(rnd.UniformInt(17) * 0.25 - 2, rnd.UniformInt(17) * 0.25 - 2, rnd.UniformInt(17) * 0.25 - 2);
                var rot = Trafo3d.RotationEulerInDegrees(rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90);
                Check(CsgM0Tests.QuadBox(Box3d.Unit),
                      CsgM0Tests.QuadBox(new Box3d(V3d.Zero, new V3d(1 + rnd.UniformInt(3) * 0.5, 1, 1))).Transformed(rot * Trafo3d.Translation(t)),
                      $"snapped {i}");
            }
            for (var i = 0; i < 1000; i++)
            {
                var t = rnd.UniformV3d() * 2.4 - new V3d(1.2);
                var rot = Trafo3d.RotationEuler(rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28);
                Check(CsgM0Tests.QuadBox(Box3d.Unit), CsgM0Tests.QuadBox(Box3d.Unit).Transformed(rot * Trafo3d.Translation(t)), $"generic {i}");
            }
            for (var i = 0; i < 200; i++)
            {
                var c = rnd.UniformV3d() * 2 - new V3d(0.5);
                Check(CsgM5Tests.Icosphere(V3d.Zero, 1.0, 2), CsgM5Tests.Icosphere(c, 0.4 + rnd.UniformDouble(), 2), $"spheres {i}");
            }
            for (var i = 0; i < 300; i++)
            {
                var snap = (i & 1) == 0;
                var t = snap
                    ? new V3d(rnd.UniformInt(13) * 0.5 - 2, rnd.UniformInt(13) * 0.5 - 2, rnd.UniformInt(9) * 0.5 - 1)
                    : rnd.UniformV3d() * 4 - new V3d(1.5);
                var rot = snap ? Trafo3d.RotationZInDegrees(rnd.UniformInt(4) * 90)
                    : Trafo3d.RotationEuler(rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28);
                Check(CsgM6Tests.LPrism(), CsgM6Tests.LPrism().Transformed(rot * Trafo3d.Translation(t)), $"lprism {i}");
            }
            Console.WriteLine($"BIGFUZZ done: {fails} failures / {total} configs");
            Assert.That(fails, Is.EqualTo(0));
        }

        [Test]
        [Explicit]
        public void SliverDump()
        {
            var path = System.IO.Path.Combine(TestContext.CurrentContext.TestDirectory, "PolyMesh", "manifold-cases.json.gz");
            using var stream = new System.IO.Compression.GZipStream(System.IO.File.OpenRead(path), System.IO.Compression.CompressionMode.Decompress);
            var doc = System.Text.Json.JsonDocument.Parse(stream);
            foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
            {
                if (c.GetProperty("name").GetString() != "cube-eps-sliver") continue;
                PolyMesh Mesh(System.Text.Json.JsonElement e)
                {
                    var vs = e.GetProperty("vertices").EnumerateArray().Select(x => x.GetDouble()).ToArray();
                    var ts = e.GetProperty("triangles").EnumerateArray().Select(x => x.GetInt32()).ToArray();
                    var pos = new V3d[vs.Length / 3].SetByIndex(i => new V3d(vs[i * 3], vs[i * 3 + 1], vs[i * 3 + 2]));
                    var fia = new int[ts.Length / 3 + 1].SetByIndex(i => i * 3);
                    return new PolyMesh { PositionArray = pos, FirstIndexArray = fia, VertexIndexArray = ts };
                }
                Environment.SetEnvironmentVariable("CSG_DEBUG_FACE", "8");
                var a = Mesh(c.GetProperty("a"));
                var b = Mesh(c.GetProperty("b"));
                var kernel = new Kernel(new Eps(1e-11));
                kernel.Ingest(a, 0);
                kernel.Ingest(b, 1);
                var pipe = new Pipeline(kernel);
                pipe.Run();
                Console.WriteLine($"tris {kernel.TriangleCount} verts {kernel.Positions.Count} frags {pipe.Fragments.Count}");
                for (var f = 0; f < pipe.Fragments.Count; f++)
                {
                    var fr = pipe.Fragments[f];
                    var ce = (kernel.Positions[fr.V0] + kernel.Positions[fr.V1] + kernel.Positions[fr.V2]) / 3;
                    Console.WriteLine($"frag {f}: mesh {kernel.TriMesh[fr.Parent]} parent {fr.Parent} verts ({fr.V0},{fr.V1},{fr.V2}) c ({ce.X:0.########},{ce.Y:0.###},{ce.Z:0.###}) {pipe.Labels[f]}");
                }
                for (var vi = 0; vi < kernel.Positions.Count; vi++)
                    Console.WriteLine($"vert {vi}: ({kernel.Positions[vi].X:0.#########},{kernel.Positions[vi].Y:0.#########},{kernel.Positions[vi].Z:0.#########}) gen {kernel.Generation[vi]}");
                // union selection: Outside everywhere + OnSame from mesh 0
                var kept = new System.Collections.Generic.List<int>();
                for (var f = 0; f < pipe.Fragments.Count; f++)
                {
                    var l = pipe.Labels[f];
                    var mesh = kernel.TriMesh[pipe.Fragments[f].Parent];
                    if (l == FragLabel.Outside || (l == FragLabel.OnSame && mesh == 0)) kept.Add(f);
                }
                var dirCount = new System.Collections.Generic.Dictionary<(int, int), System.Collections.Generic.List<int>>();
                foreach (var f in kept)
                {
                    var fr = pipe.Fragments[f];
                    foreach (var (u, v) in new[] { (fr.V0, fr.V1), (fr.V1, fr.V2), (fr.V2, fr.V0) })
                    {
                        if (!dirCount.TryGetValue((u, v), out var list)) dirCount[(u, v)] = list = new();
                        list.Add(f);
                    }
                }
                foreach (var kvp in dirCount.Where(kv => kv.Value.Count > 1))
                    Console.WriteLine($"DUP directed edge {kvp.Key}: frags {string.Join(",", kvp.Value)}");
                try { Csg.Union(a, b); Console.WriteLine("union ok"); }
                catch (Exception e2) { Console.WriteLine($"union FAIL: {e2.Message}"); }
            }
        }

        [Test]
        [Explicit]
        public void PerfStages()
        {
            Environment.SetEnvironmentVariable("CSG_PERF", "1");
            // gyroid-like stress via two warped high-res spheres + big spheres
            var a6 = CsgM5Tests.Icosphere(V3d.Zero, 1.0, 6);
            var b6 = CsgM5Tests.Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, 6);
            Console.WriteLine("== sphere subdiv6 union (2x81920 tris) ==");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            Csg.Union(a6, b6);
            Console.WriteLine($"TOTAL {sw.ElapsedMilliseconds} ms");
            Environment.SetEnvironmentVariable("CSG_PERF", null);
        }

        [Test]
        [Explicit]
        public void Perf()
        {
            foreach (var sub in new[] { 4, 5, 6 })
            {
                var a = CsgM5Tests.Icosphere(V3d.Zero, 1.0, sub);
                var b = CsgM5Tests.Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, sub);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var union = Csg.Union(a, b);
                sw.Stop();
                var tris = (a.FirstIndexArray.Length - 1) * 2;
                Console.WriteLine($"PERF union {tris} tris: {sw.ElapsedMilliseconds} ms ({union.Sum(m => m.FirstIndexArray.Length - 1)} out tris)");
            }
        }
    }
}
