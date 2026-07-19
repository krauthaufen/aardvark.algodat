# csg-diff — differential testing against Manifold

Compares Aardvark.PolyMesh.Csg boolean results against the
[Manifold](https://github.com/elalish/manifold) library on a deterministic zoo
of fuzzed solid pairs (generic + snapped boxes, spheres, concave L-prisms).

Volume is compared tightly (1e-8 relative). Surface area is compared only for
cases without coincident-surface contact — at measure-zero contact the two
libraries legitimately keep different degenerate sheets.

## Setup (once)

```bash
python3 -m venv ~/.venvs/csgdiff
~/.venvs/csgdiff/bin/pip install manifold3d numpy
```

## Run

```bash
# 1. export cases + our results (writes /tmp/csgdiff-cases.json)
dotnet test src/Aardvark.Algodat.Tests -c Release \
  --filter "FullyQualifiedName~CsgDifferential" -- NUnit.Where="cat==Explicit or cat!=Explicit"

# 2. compare against manifold3d
~/.venvs/csgdiff/bin/python tools/csg-diff.py
```

Exit code 0 = all cases agree.
