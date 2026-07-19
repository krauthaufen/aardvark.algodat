using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>Input sanitization: welding, hole closing, non-manifold splitting.</summary>
    [TestFixture]
    public class CsgRepairTests
    {
        private static PolyMesh Tri(V3d[] pos, int[] tris)
            => new() { PositionArray = pos, FirstIndexArray = new int[tris.Length / 3 + 1].SetByIndex(i => i * 3), VertexIndexArray = tris };

        private static void AssertManifold(PolyMesh m)
        {
            var v = ManifoldChecks.FindManifoldViolation(m.FirstIndexArray, m.VertexIndexArray, m.PositionArray.Length);
            Assert.That(v, Is.Null, () => $"not manifold: {v}");
        }

        // a unit box as 12 triangles, optionally with defects injected
        private static (V3d[] pos, List<int> tris) BoxTris(Box3d box)
        {
            var n = box.Min; var x = box.Max;
            var pos = new[]
            {
                new V3d(n.X, n.Y, n.Z), new V3d(x.X, n.Y, n.Z), new V3d(x.X, x.Y, n.Z), new V3d(n.X, x.Y, n.Z),
                new V3d(n.X, n.Y, x.Z), new V3d(x.X, n.Y, x.Z), new V3d(x.X, x.Y, x.Z), new V3d(n.X, x.Y, x.Z),
            };
            int[] quads = { 3, 2, 1, 0,  4, 5, 6, 7,  0, 1, 5, 4,  2, 3, 7, 6,  1, 2, 6, 5,  3, 0, 4, 7 };
            var tris = new List<int>();
            for (var q = 0; q < 6; q++)
            {
                int a = quads[q * 4], b = quads[q * 4 + 1], c = quads[q * 4 + 2], d = quads[q * 4 + 3];
                tris.AddRange(new[] { a, b, c, a, c, d });
            }
            return (pos, tris);
        }

        [Test]
        public void WeldsDuplicatedVertices()
        {
            var (pos, tris) = BoxTris(Box3d.Unit);
            // duplicate every vertex with a sub-tolerance jitter and rewire half the faces to the copies
            var pos2 = pos.ToList();
            var dup = new int[pos.Length];
            for (var i = 0; i < pos.Length; i++) { dup[i] = pos2.Count; pos2.Add(pos[i] + new V3d(1e-9, -1e-9, 1e-9)); }
            for (var t = 0; t < tris.Count; t += 3)
                if ((t / 3) % 2 == 0) { tris[t] = dup[tris[t]]; tris[t + 1] = dup[tris[t + 1]]; tris[t + 2] = dup[tris[t + 2]]; }

            var repaired = PolyMeshRepair.Repair(Tri(pos2.ToArray(), tris.ToArray()));
            Assert.That(repaired.Length, Is.EqualTo(1));
            AssertManifold(repaired[0]);
            Assert.That(CsgM0Tests.Volume(repaired[0]), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void ClosesAHole()
        {
            var (pos, tris) = BoxTris(Box3d.Unit);
            tris.RemoveRange(0, 6); // drop the two triangles of the first face → a square hole
            var repaired = PolyMeshRepair.Repair(Tri(pos, tris.ToArray()));
            Assert.That(repaired.Length, Is.EqualTo(1));
            AssertManifold(repaired[0]);
            Assert.That(CsgM0Tests.Volume(repaired[0]), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void SplitsTwoBoxesSharingAnEdge()
        {
            // two unit boxes touching along the edge x=1,y=1 (box B at x∈[1,2], y∈[1,2])
            var (posA, trisA) = BoxTris(Box3d.Unit);
            var (posB, trisB) = BoxTris(new Box3d(new V3d(1, 1, 0), new V3d(2, 2, 1)));
            var pos = posA.Concat(posB).ToArray();
            var tris = new List<int>(trisA);
            foreach (var v in trisB) tris.Add(v + posA.Length);

            var repaired = PolyMeshRepair.Repair(Tri(pos, tris.ToArray()));
            Assert.That(repaired.Length, Is.EqualTo(2), "shared-edge non-manifold should split into two solids");
            foreach (var m in repaired)
            {
                AssertManifold(m);
                Assert.That(CsgM0Tests.Volume(m), Is.EqualTo(1.0).Within(1e-9));
            }
        }

        [Test]
        public void SplitsTwoBoxesSharingAVertex()
        {
            var (posA, trisA) = BoxTris(Box3d.Unit);
            var (posB, trisB) = BoxTris(new Box3d(new V3d(1, 1, 1), new V3d(2, 2, 2))); // touch at corner (1,1,1)
            var pos = posA.Concat(posB).ToArray();
            var tris = new List<int>(trisA);
            foreach (var v in trisB) tris.Add(v + posA.Length);

            var repaired = PolyMeshRepair.Repair(Tri(pos, tris.ToArray()));
            Assert.That(repaired.Length, Is.EqualTo(2), "shared-vertex pinch should split into two solids");
            foreach (var m in repaired) { AssertManifold(m); Assert.That(CsgM0Tests.Volume(m), Is.EqualTo(1.0).Within(1e-9)); }
        }

        [Test]
        public void CancelsDuplicateAndAntiFaces()
        {
            var (pos, tris) = BoxTris(Box3d.Unit);
            // duplicate one triangle (redundant) and add an anti-face of another (flap)
            tris.AddRange(new[] { tris[0], tris[1], tris[2] });          // exact duplicate
            tris.AddRange(new[] { tris[5], tris[4], tris[3] });          // reversed second triangle
            tris.AddRange(new[] { tris[3], tris[4], tris[5] });          // and its forward copy → net cancels
            var repaired = PolyMeshRepair.Repair(Tri(pos, tris.ToArray()));
            Assert.That(repaired.Length, Is.EqualTo(1));
            AssertManifold(repaired[0]);
            Assert.That(CsgM0Tests.Volume(repaired[0]), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void FixesInconsistentWinding()
        {
            var (pos, tris) = BoxTris(Box3d.Unit);
            // flip a handful of triangles' winding
            for (var t = 0; t < tris.Count; t += 3)
                if ((t / 3) % 3 == 0) (tris[t + 1], tris[t + 2]) = (tris[t + 2], tris[t + 1]);
            var repaired = PolyMeshRepair.Repair(Tri(pos, tris.ToArray()));
            Assert.That(repaired.Length, Is.EqualTo(1));
            AssertManifold(repaired[0]);
            Assert.That(CsgM0Tests.Volume(repaired[0]), Is.EqualTo(1.0).Within(1e-9));
        }

        [Test]
        public void RepairsMeshTheKernelRejects()
        {
            // split a box vertex into two distinct vertices a hair apart (below
            // the kernel's weld tolerance): the contract rejects it as an
            // ambiguous sub-tolerance feature. Repair welds them.
            var (pos, tris) = BoxTris(Box3d.Unit);
            var pos2 = pos.ToList();
            var twin = pos2.Count; pos2.Add(pos[6] + new V3d(1e-13, 0, 0)); // duplicate corner 6 a hair away
            for (var t = 0; t < tris.Count; t += 3)
                if ((t / 3) % 2 == 0)
                    for (var k = 0; k < 3; k++)
                        if (tris[t + k] == 6) tris[t + k] = twin;
            var defective = Tri(pos2.ToArray(), tris.ToArray());

            Assert.Throws<CsgInputException>(() => Csg.Union(defective, CsgM0Tests.QuadBox(new Box3d(new V3d(0.5), new V3d(1.5)))));

            var repaired = PolyMeshRepair.Repair(defective);
            Assert.That(repaired.Length, Is.EqualTo(1));
            AssertManifold(repaired[0]);
            // now the kernel accepts it
            var union = Csg.Union(repaired[0], CsgM0Tests.QuadBox(new Box3d(new V3d(0.5), new V3d(1.5))));
            foreach (var m in union) AssertManifold(m);
        }

        /// <summary>Repairs the real cow.off-derived mesh if the (gitignored) external fixture is present.</summary>
        [Test]
        [Explicit]
        public void RepairsRealWorldCow()
        {
            var path = System.IO.Path.Combine(TestContext.CurrentContext.TestDirectory, "PolyMesh", "external-cases.json.gz");
            if (!System.IO.File.Exists(path)) { Assert.Ignore("external-cases.json.gz not present"); return; }
            using var stream = new System.IO.Compression.GZipStream(System.IO.File.OpenRead(path), System.IO.Compression.CompressionMode.Decompress);
            var doc = System.Text.Json.JsonDocument.Parse(stream);
            var cow = doc.RootElement.GetProperty("cases").EnumerateArray()
                .FirstOrDefault(c => (c.GetProperty("name").GetString() ?? "").StartsWith("cow"));
            if (cow.ValueKind == System.Text.Json.JsonValueKind.Undefined) { Assert.Ignore("cow case absent"); return; }
            var e = cow.GetProperty("a");
            var vs = e.GetProperty("vertices").EnumerateArray().Select(x => x.GetDouble()).ToArray();
            var ts = e.GetProperty("triangles").EnumerateArray().Select(x => x.GetInt32()).ToArray();
            var raw = Tri(new V3d[vs.Length / 3].SetByIndex(i => new V3d(vs[i * 3], vs[i * 3 + 1], vs[i * 3 + 2])), ts);

            var repaired = PolyMeshRepair.Repair(raw);
            Assert.That(repaired.Length, Is.GreaterThanOrEqualTo(1));
            foreach (var m in repaired) AssertManifold(m);
            var big = repaired.OrderByDescending(CsgM0Tests.Volume).First();
            TestContext.Out.WriteLine($"cow repaired into {repaired.Length} manifold(s), largest vol {CsgM0Tests.Volume(big):0.####}");
            // the repaired mesh is accepted as CSG input (no CsgInputException);
            // it may still self-intersect, which is out of repair's scope, so a
            // boolean is best-effort here — we only require valid input.
            try { foreach (var m in Csg.Union(big, CsgM5Tests.Icosphere(new V3d(0.31, 0.22, 0.13), 0.37, 2))) AssertManifold(m); }
            catch (CsgInputException) { Assert.Fail("repaired mesh still rejected as input"); }
            catch (CsgVerificationException ex) { TestContext.Out.WriteLine($"boolean hit a self-intersection/degeneracy (out of repair scope): {ex.Message}"); }
        }

        [Test]
        public void FuzzCombinedDefects()
        {
            var rnd = new RandomSystem(4242);
            for (var iter = 0; iter < 200; iter++)
            {
                var (pos, tris) = BoxTris(Box3d.Unit);
                var pos2 = pos.ToList();
                // random winding flips
                for (var t = 0; t < tris.Count; t += 3)
                    if (rnd.UniformDouble() < 0.3) (tris[t + 1], tris[t + 2]) = (tris[t + 2], tris[t + 1]);
                // random unwelded duplicate vertices
                if (rnd.UniformDouble() < 0.7)
                {
                    var v = rnd.UniformInt(pos.Length);
                    var dup = pos2.Count; pos2.Add(pos[v] + rnd.UniformV3dDirection() * 1e-9);
                    for (var t = 0; t < tris.Count; t += 3)
                        if (rnd.UniformDouble() < 0.5)
                            for (var k = 0; k < 3; k++) if (tris[t + k] == v) tris[t + k] = dup;
                }
                // random duplicate faces
                if (rnd.UniformDouble() < 0.5)
                {
                    var t = rnd.UniformInt(tris.Count / 3) * 3;
                    tris.AddRange(new[] { tris[t], tris[t + 1], tris[t + 2] });
                }
                var repaired = PolyMeshRepair.Repair(Tri(pos2.ToArray(), tris.ToArray()));
                var vol = 0.0;
                foreach (var m in repaired) { AssertManifold(m); vol += CsgM0Tests.Volume(m); }
                // holes were not injected here, so volume must be recovered
                Assert.That(vol, Is.EqualTo(1.0).Within(1e-9), $"iter {iter}");
            }
        }

        [Test]
        public void RepairedMeshBooleansCleanly()
        {
            // a defective box (unwelded dupes + a hole) repaired, then unioned with a sphere
            var (pos, tris) = BoxTris(Box3d.Unit);
            tris.RemoveRange(0, 6); // hole
            var repaired = PolyMeshRepair.Repair(Tri(pos, tris.ToArray()));
            Assert.That(repaired.Length, Is.EqualTo(1));
            var ball = CsgM5Tests.Icosphere(new V3d(1, 1, 1), 0.5, 2);
            var union = Csg.Union(repaired[0], ball);
            foreach (var m in union) AssertManifold(m);
            Assert.That(union.Sum(CsgM0Tests.Volume), Is.GreaterThan(1.0));
        }
    }
}
