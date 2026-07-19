using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>MaxThreads: identical, deterministic results at every thread count.</summary>
    [TestFixture]
    public class CsgThreadingTests
    {
        [Test]
        public void ThreadCountDoesNotChangeResults()
        {
            var a = CsgM5Tests.Icosphere(V3d.Zero, 1.0, 3);
            var b = CsgM5Tests.Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, 3);

            var sequential = Csg.Union(a, b, new CsgOptions { MaxThreads = 1 });
            foreach (var threads in new[] { 2, 4, Environment.ProcessorCount })
            {
                var parallel = Csg.Union(a, b, new CsgOptions { MaxThreads = threads });
                Assert.That(parallel.Length, Is.EqualTo(sequential.Length), $"threads {threads}");
                for (var i = 0; i < parallel.Length; i++)
                {
                    Assert.That(parallel[i].VertexIndexArray, Is.EqualTo(sequential[i].VertexIndexArray), $"threads {threads}");
                    Assert.That(parallel[i].PositionArray, Is.EqualTo(sequential[i].PositionArray), $"threads {threads}");
                }
            }
        }

        [Test]
        public void ParallelFailuresKeepTheirType()
        {
            var box = CsgM0Tests.QuadBox(Box3d.Unit);
            var open = new PolyMesh
            {
                PositionArray = box.PositionArray,
                FirstIndexArray = box.FirstIndexArray.Take(6).ToArray(),
                VertexIndexArray = box.VertexIndexArray.Take(20).ToArray(),
            };
            var other = CsgM0Tests.QuadBox(Box3d.Unit.Translated(new V3d(10, 0, 0)));
            Assert.Throws<CsgInputException>(
                () => Csg.Union(open, other, new CsgOptions { MaxThreads = 4 }));
        }

        [Test]
        public void ParallelFuzzMatchesSequential()
        {
            var rnd = new RandomSystem(555);
            for (var i = 0; i < 20; i++)
            {
                var t = new V3d(rnd.UniformInt(9) * 0.25 - 1, rnd.UniformInt(9) * 0.25 - 1, rnd.UniformInt(9) * 0.25 - 1);
                var rot = Trafo3d.RotationEulerInDegrees(rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90);
                var a = CsgM0Tests.QuadBox(Box3d.Unit);
                var b = CsgM0Tests.QuadBox(Box3d.Unit).Transformed(rot * Trafo3d.Translation(t));
                var seq = Csg.Union(a, b, new CsgOptions { MaxThreads = 1 }).Sum(CsgM0Tests.Volume);
                var par = Csg.Union(a, b, new CsgOptions { MaxThreads = 8 }).Sum(CsgM0Tests.Volume);
                Assert.That(par, Is.EqualTo(seq).Within(1e-12), $"iteration {i}");
            }
        }
    }
}
