"""Tests for Murphy / Brier bin decomposition."""

from __future__ import annotations

import numpy as np

from src.common.scoring_rules import murphy_decomposition_from_bins


def test_murphy_partition_climatology_consistency() -> None:
    """Toy bins: decomposition should reproduce mean Brier when bins are exact."""
    # Two bins, forecasts perfectly calibrated within bin
    n_k = np.array([50.0, 50.0])
    p_bar = np.array([0.25, 0.75])
    o_bar_k = np.array([0.25, 0.75])
    m = murphy_decomposition_from_bins(n_k, p_bar, o_bar_k)
    assert m.o_bar == 0.5
    # Reliability zero, resolution positive
    assert abs(m.reliability) < 1e-9
    assert m.resolution > 0
    assert abs(m.uncertainty - 0.25) < 1e-9
    # Brier from partition equals reliability - resolution + uncertainty
    assert abs(m.brier_from_partition - (m.reliability - m.resolution + m.uncertainty)) < 1e-9


def test_murphy_miscalibrated_bins() -> None:
    n_k = np.array([100.0])
    p_bar = np.array([0.5])
    o_bar_k = np.array([0.6])
    m = murphy_decomposition_from_bins(n_k, p_bar, o_bar_k)
    assert abs(m.reliability - 0.01) < 1e-9
    assert abs(m.resolution) < 1e-9  # single bin => no between-bin resolution
    assert abs(m.uncertainty - 0.24) < 1e-9
    assert abs(m.brier_from_partition - 0.25) < 1e-9  # = (0.5-0.6)^2 + Var(Bernoulli(0.6))
