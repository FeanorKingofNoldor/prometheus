"""Cross-engine signal consumers.

This package wires Apatheon's intelligence outputs (divergence, convergence
timing, compound-attack pressure, beneficiary scoring) into Prometheus's
decision pipeline.  Each module persists the upstream signal locally and
emits a ``DIVERGENCE`` / ``CONVERGENCE`` / etc. row in ``engine_decisions``
so the Meta-Orchestrator can score realised outcomes against the
prediction the same way it scores assessment or portfolio decisions.
"""
