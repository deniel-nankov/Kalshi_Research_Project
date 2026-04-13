"""Proper scoring rule helpers (Murphy / Brier decomposition on binned forecasts)."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class MurphyBinDecomposition:
    """Murphy (1973) partition using *binned* forecasts (means within probability bins)."""

    n_total: int
    o_bar: float
    reliability: float
    resolution: float
    uncertainty: float
    brier_from_partition: float

    def as_dict(self) -> dict[str, float | int]:
        return {
            "n_total": self.n_total,
            "o_bar": self.o_bar,
            "reliability": self.reliability,
            "resolution": self.resolution,
            "uncertainty": self.uncertainty,
            "brier_from_partition": self.brier_from_partition,
        }


def murphy_decomposition_from_bins(
    n_k: np.ndarray,
    p_bar_k: np.ndarray,
    o_bar_k: np.ndarray,
) -> MurphyBinDecomposition:
    """Compute reliability, resolution, uncertainty for binary outcomes.

    ``n_k`` counts per bin, ``p_bar_k`` mean forecast in bin, ``o_bar_k`` mean outcome in bin.
    """
    n = np.asarray(n_k, dtype=float)
    p = np.asarray(p_bar_k, dtype=float)
    o = np.asarray(o_bar_k, dtype=float)
    N = float(np.sum(n))
    if N <= 0:
        raise ValueError("empty bins")
    w = n / N
    o_bar = float(np.sum(w * o))
    reliability = float(np.sum(w * (p - o) ** 2))
    resolution = float(np.sum(w * (o - o_bar) ** 2))
    uncertainty = o_bar * (1.0 - o_bar)
    brier_part = reliability - resolution + uncertainty
    return MurphyBinDecomposition(
        n_total=int(np.sum(n)),
        o_bar=o_bar,
        reliability=reliability,
        resolution=resolution,
        uncertainty=uncertainty,
        brier_from_partition=brier_part,
    )
