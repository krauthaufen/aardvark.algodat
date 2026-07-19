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
                    Console.WriteLine($"frag {f}: mesh {kernel.TriMesh[fr.Parent]} parent {fr.Parent} verts ({fr.V0},{fr.V1},{fr.V2}) c ({ce.X:0.########},{ce.Y:0.###},{ce.Z:0.###}) {pipe.Label(f, 1 - kernel.TriMesh[fr.Parent])}");
                }
                for (var vi = 0; vi < kernel.Positions.Count; vi++)
                    Console.WriteLine($"vert {vi}: ({kernel.Positions[vi].X:0.#########},{kernel.Positions[vi].Y:0.#########},{kernel.Positions[vi].Z:0.#########}) f {kernel.TolFactor[vi]:0.#}");
                // union selection: Outside everywhere + OnSame from mesh 0
                var kept = new System.Collections.Generic.List<int>();
                for (var f = 0; f < pipe.Fragments.Count; f++)
                {
                    var mesh = kernel.TriMesh[pipe.Fragments[f].Parent];
                    var l = pipe.Label(f, 1 - mesh);
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
        public void NaryDump()
        {
            var a = CsgM0Tests.QuadBox(new Box3d(new V3d(0, 0, 0), new V3d(2, 2, 1)));
            var holes = new[]
            {
                CsgM0Tests.QuadBox(new Box3d(new V3d(0.5, 0.5, -0.5), new V3d(1.25, 1.5, 1.5))),
                CsgM0Tests.QuadBox(new Box3d(new V3d(0.75, 0.5, -0.5), new V3d(1.5, 1.5, 1.5))),
            };
            var kernel = new Kernel(new Eps(1e-11));
            kernel.Ingest(a, 0);
            kernel.Ingest(holes[0], 1);
            kernel.Ingest(holes[1], 2);
            var pipe = new Pipeline(kernel);
            pipe.Run();
            // emulate Difference selection and find open edges
            var kept = new System.Collections.Generic.List<(int F, bool Flip)>();
            for (var f = 0; f < pipe.Fragments.Count; f++)
            {
                var mi = kernel.TriMesh[pipe.Fragments[f].Parent];
                if (mi == 0)
                {
                    var ok = true;
                    for (var m = 1; m <= 2 && ok; m++)
                        ok = pipe.Label(f, m) is FragLabel.Outside or FragLabel.OnOpposite;
                    if (ok) kept.Add((f, false));
                }
                else
                {
                    if (pipe.Label(f, 0) != FragLabel.Inside) continue;
                    var other = mi == 1 ? 2 : 1;
                    var l = pipe.Label(f, other);
                    var ok = l == FragLabel.Outside || (l == FragLabel.OnSame && other > mi);
                    if (ok) kept.Add((f, true));
                }
            }
            var dir = new System.Collections.Generic.Dictionary<(int, int), int>();
            foreach (var (f, flip) in kept)
            {
                var fr = pipe.Fragments[f];
                var (v0, v1, v2) = flip ? (fr.V0, fr.V2, fr.V1) : (fr.V0, fr.V1, fr.V2);
                foreach (var (u, v) in new[] { (v0, v1), (v1, v2), (v2, v0) })
                    dir[(u, v)] = f;
            }
            foreach (var kvp in dir)
            {
                var (u, v) = kvp.Key;
                if (dir.ContainsKey((v, u))) continue;
                Console.WriteLine($"OPEN {u}->{v}: {kernel.Positions[u]} -> {kernel.Positions[v]} (frag {kvp.Value})");
                // print fragments touching this edge
                for (var f = 0; f < pipe.Fragments.Count; f++)
                {
                    var fr = pipe.Fragments[f];
                    var vs = new[] { fr.V0, fr.V1, fr.V2 };
                    if (!vs.Contains(u) || !vs.Contains(v)) continue;
                    var mi = kernel.TriMesh[fr.Parent];
                    Console.WriteLine($"   frag {f} mesh {mi} verts ({fr.V0},{fr.V1},{fr.V2}) " +
                        $"labels [{pipe.Label(f, 0)},{pipe.Label(f, 1)},{pipe.Label(f, 2)}]");
                }
            }
        }

        [Test]
        [Explicit]
        public void BenchExport()
        {
            // export sphere pairs and time our union on them (after warmup)
            var cases = new System.Collections.Generic.List<object>();
            foreach (var sub in new[] { 4, 5, 6 })
            {
                var a = CsgM5Tests.Icosphere(V3d.Zero, 1.0, sub);
                var b = CsgM5Tests.Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, sub);
                Csg.Union(a, b); // warmup / JIT
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var u = Csg.Union(a, b);
                sw.Stop();
                Console.WriteLine($"BENCH ours sphere-{sub} ({(a.FirstIndexArray.Length - 1) * 2} tris): {sw.Elapsed.TotalMilliseconds:0.0} ms");
                object Mesh(PolyMesh m) => new
                {
                    vertices = m.PositionArray.SelectMany(p => new[] { p.X, p.Y, p.Z }).ToArray(),
                    triangles = m.VertexIndexArray,
                };
                cases.Add(new { name = $"sphere-{sub}", a = Mesh(a), b = Mesh(b) });
            }
            System.IO.File.WriteAllText("/tmp/csgbench.json",
                System.Text.Json.JsonSerializer.Serialize(new { cases }));
            Console.WriteLine("BENCH exported /tmp/csgbench.json");
        }

        [Test]
        [Explicit]
        public void Bench()
        {
            double Median(double[] xs) { Array.Sort(xs); return xs[xs.Length / 2]; }

            double Measure(Action op, int warmup = 3, int iterations = 9)
            {
                for (var i = 0; i < warmup; i++) op();
                var times = new double[iterations];
                for (var i = 0; i < iterations; i++)
                {
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    op();
                    times[i] = sw.Elapsed.TotalMilliseconds;
                }
                return Median(times);
            }

            var seq = new CsgOptions { MaxThreads = 1 };
            var seqIo = new CsgOptions { MaxThreads = 1, Verification = CsgVerification.InputOnly };
            var par = new CsgOptions();
            var parIo = new CsgOptions { Verification = CsgVerification.InputOnly };
            Console.WriteLine($"{"case",-14} {"tris",8} {"poly-1t",9} {"prep-1t",9} {"prep-1t-io",10} {"poly-mt",9} {"prep-mt",9} {"prep-mt-io",10}");
            foreach (var sub in new[] { 3, 4, 5, 6 })
            {
                var pa = CsgM5Tests.Icosphere(V3d.Zero, 1.0, sub);
                var pb = CsgM5Tests.Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, sub);
                var a = CsgMesh.FromPolyMesh(pa);
                var b = CsgMesh.FromPolyMesh(pb);
                var tris = (pa.FirstIndexArray.Length - 1) * 2;
                var m1 = Measure(() => Csg.Union(pa, pb, seq));
                var m2 = Measure(() => Csg.Union(a, b, seq));
                var m3 = Measure(() => Csg.Union(a, b, seqIo));
                var m4 = Measure(() => Csg.Union(pa, pb, par));
                var m5 = Measure(() => Csg.Union(a, b, par));
                var m6 = Measure(() => Csg.Union(a, b, parIo));
                Console.WriteLine($"{"sphere-" + sub,-14} {tris,8} {m1,8:0.0}m {m2,8:0.0}m {m3,9:0.0}m {m4,8:0.0}m {m5,8:0.0}m {m6,9:0.0}m");
            }
        }

        [Test]
        [Explicit]
        public void TangentDump()
        {
            var path = System.IO.Path.Combine(TestContext.CurrentContext.TestDirectory, "PolyMesh", "manifold-cases.json.gz");
            using var stream = new System.IO.Compression.GZipStream(System.IO.File.OpenRead(path), System.IO.Compression.CompressionMode.Decompress);
            var doc = System.Text.Json.JsonDocument.Parse(stream);
            foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
            {
                if (c.GetProperty("name").GetString() != "sphere-near-tangent") continue; // tangent case
                PolyMesh Mesh(System.Text.Json.JsonElement e)
                {
                    var vs = e.GetProperty("vertices").EnumerateArray().Select(x => x.GetDouble()).ToArray();
                    var ts = e.GetProperty("triangles").EnumerateArray().Select(x => x.GetInt32()).ToArray();
                    var pos = new V3d[vs.Length / 3].SetByIndex(i => new V3d(vs[i * 3], vs[i * 3 + 1], vs[i * 3 + 2]));
                    var fia = new int[ts.Length / 3 + 1].SetByIndex(i => i * 3);
                    return new PolyMesh { PositionArray = pos, FirstIndexArray = fia, VertexIndexArray = ts };
                }
                var a = Mesh(c.GetProperty("a"));
                var b = Mesh(c.GetProperty("b"));
                foreach (var mt in new[] { 1, 8 })
                    foreach (var (opName, op) in new (string, Func<CsgOptions, PolyMesh[]>)[]
                    {
                        ("union", o => Csg.Union(a, b, o)), ("inter", o => Csg.Intersection(a, b, o)), ("diff", o => Csg.Difference(a, b, o)),
                    })
                    {
                        try { var r = op(new CsgOptions { MaxThreads = mt }); Console.WriteLine($"API {opName} mt={mt}: ok ({r.Sum(m => m.FirstIndexArray.Length - 1)} tris)"); }
                        catch (Exception ex) { Console.WriteLine($"API {opName} mt={mt}: FAIL {ex.Message}"); }
                    }
                var kernel = new Kernel(new Eps(1e-11));
                kernel.Ingest(a, 0);
                kernel.Ingest(b, 1);
                var pipe = new Pipeline(kernel, maxThreads: 1);
                pipe.Run();
                foreach (var (op, sel) in new (string, Func<int, FragLabel, bool>)[]
                {
                    ("union", (mi, l) => l == FragLabel.Outside || (l == FragLabel.OnSame && mi == 0)),
                    ("inter", (mi, l) => l == FragLabel.Inside || (l == FragLabel.OnSame && mi == 0)),
                    ("diffA", (mi, l) => mi == 0 ? l is FragLabel.Outside or FragLabel.OnOpposite : l == FragLabel.Inside),
                })
                {
                var kept = new System.Collections.Generic.List<int>();
                for (var f = 0; f < pipe.Fragments.Count; f++)
                {
                    var mi = kernel.TriMesh[pipe.Fragments[f].Parent];
                    var l = pipe.Label(f, 1 - mi);
                    if (sel(mi, l)) kept.Add(f);
                }
                Console.WriteLine($"== {op}: {kept.Count} fragments");
                var dir = new System.Collections.Generic.Dictionary<(int, int), int>();
                foreach (var f in kept)
                {
                    var fr = pipe.Fragments[f];
                    foreach (var (u, v) in new[] { (fr.V0, fr.V1), (fr.V1, fr.V2), (fr.V2, fr.V0) })
                        dir[(u, v)] = f;
                }
                if (op == "union")
                {
                    Console.WriteLine("fragments at kernel edge (2054,2055):");
                    for (var f = 0; f < pipe.Fragments.Count; f++)
                    {
                        var fr = pipe.Fragments[f];
                        var vs3 = new[] { fr.V0, fr.V1, fr.V2 };
                        if (!vs3.Contains(2054) || !vs3.Contains(2055)) continue;
                        var mi = kernel.TriMesh[fr.Parent];
                        var p0 = kernel.Positions[fr.V0];
                        var wn = (kernel.Positions[fr.V1] - p0).Cross(kernel.Positions[fr.V2] - p0).Normalized;
                        Console.WriteLine($"   frag {f} mesh {mi} parent {fr.Parent} verts ({fr.V0},{fr.V1},{fr.V2}) " +
                            $"labels[{pipe.Label(f, 0)},{pipe.Label(f, 1)}] kept={kept.Contains(f)} n=({wn.X:0.###},{wn.Y:0.###},{wn.Z:0.###})");
                    }
                }
                var shown = 0;
                foreach (var kvp in dir)
                {
                    var (u, v) = kvp.Key;
                    if (dir.ContainsKey((v, u))) continue;
                    if (shown++ >= 3) break;
                    Console.WriteLine($"OPEN {u}->{v}: {kernel.Positions[u]} -> {kernel.Positions[v]} f[{kernel.TolFactor[u]:0.#},{kernel.TolFactor[v]:0.#}]");
                    for (var f = 0; f < pipe.Fragments.Count; f++)
                    {
                        var fr = pipe.Fragments[f];
                        var vs2 = new[] { fr.V0, fr.V1, fr.V2 };
                        if (!vs2.Contains(u) || !vs2.Contains(v)) continue;
                        var mi = kernel.TriMesh[fr.Parent];
                        Console.WriteLine($"   frag {f} mesh {mi} parent {fr.Parent} verts ({fr.V0},{fr.V1},{fr.V2}) " +
                            $"labels [{pipe.Label(f, 0)},{pipe.Label(f, 1)}] kept={kept.Contains(f)}");
                    }
                }
                }
            }
        }

        [Test]
        [Explicit]
        public void SelfDump()
        {
            var path = System.IO.Path.Combine(TestContext.CurrentContext.TestDirectory, "PolyMesh", "external-cases.json.gz");
            using var stream = new System.IO.Compression.GZipStream(System.IO.File.OpenRead(path), System.IO.Compression.CompressionMode.Decompress);
            var doc = System.Text.Json.JsonDocument.Parse(stream);
            foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
            {
                if (c.GetProperty("name").GetString() != Environment.GetEnvironmentVariable("CSG_CASE")) continue;
                PolyMesh Mesh(System.Text.Json.JsonElement e)
                {
                    var vs = e.GetProperty("vertices").EnumerateArray().Select(x => x.GetDouble()).ToArray();
                    var ts = e.GetProperty("triangles").EnumerateArray().Select(x => x.GetInt32()).ToArray();
                    var pos = new V3d[vs.Length / 3].SetByIndex(i => new V3d(vs[i * 3], vs[i * 3 + 1], vs[i * 3 + 2]));
                    var fia = new int[ts.Length / 3 + 1].SetByIndex(i => i * 3);
                    return new PolyMesh { PositionArray = pos, FirstIndexArray = fia, VertexIndexArray = ts };
                }
                var a = Mesh(c.GetProperty("a"));
                var b = Mesh(c.GetProperty("b"));
                Environment.SetEnvironmentVariable("CSG_DEBUG_REG", "1");
                var kernel = new Kernel(new Eps(1e-11));
                kernel.Ingest(a, 0);
                kernel.Ingest(b, 1);
                var pipe = new Pipeline(kernel, maxThreads: 1);
                try { pipe.Run(); Console.WriteLine("pipeline ok"); }
                catch (Exception ex) { Console.WriteLine($"pipeline FAIL: {ex.Message}"); }
            }
        }

        [Test]
        [Explicit]
        public void PerfPrepared()
        {
            var a = CsgMesh.FromPolyMesh(CsgM5Tests.Icosphere(V3d.Zero, 1.0, 6));
            var b = CsgMesh.FromPolyMesh(CsgM5Tests.Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, 6));
            var o = new CsgOptions { Verification = CsgVerification.InputOnly };
            for (var i = 0; i < 4; i++) Csg.Union(a, b, o); // warmup
            Environment.SetEnvironmentVariable("CSG_PERF", "1");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            Csg.Union(a, b, o);
            Console.WriteLine($"TOTAL prepared union 2x81920 tris: {sw.ElapsedMilliseconds} ms");
            Environment.SetEnvironmentVariable("CSG_PERF", null);
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
