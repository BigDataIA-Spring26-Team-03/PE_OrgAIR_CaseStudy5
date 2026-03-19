# src/services/retrieval/dimension_mapper.py

from __future__ import annotations

from typing import Dict, List, Tuple

from src.scoring.evidence_mapper import (
    Dimension,
    SIGNAL_TO_DIMENSION_MAP,
    SignalSource,
)
from src.services.integration.cs2_client import SignalCategory, SourceType


# ---------------------------------------------------------------------------
# Signal category → dimension weights
#
# Derived dynamically from CS3's SIGNAL_TO_DIMENSION_MAP so weight changes
# in the scoring engine propagate here automatically.
# ---------------------------------------------------------------------------

_CATEGORY_WEIGHTS: Dict[SignalCategory, Tuple[Dimension, Dict[Dimension, float]]] = {}

for _source, _mapping in SIGNAL_TO_DIMENSION_MAP.items():
    try:
        _category = SignalCategory(_source.value)
        _CATEGORY_WEIGHTS[_category] = (
            _mapping.primary_dimension,
            {
                _mapping.primary_dimension: float(_mapping.primary_weight),
                **{d: float(w) for d, w in _mapping.secondary_mappings.items()},
            },
        )
    except ValueError:
        pass  


# ---------------------------------------------------------------------------
# Source type → signal category
#
# Resolves a CS2 SourceType to its SignalCategory so callers that only
# weights via a two-step lookup:
#     source_type → signal_category → dimension weights
# ---------------------------------------------------------------------------

_SOURCE_TO_CATEGORY: Dict[SourceType, SignalCategory] = {
    SourceType.SEC_10K_ITEM_1:          SignalCategory.DIGITAL_PRESENCE,
    SourceType.SEC_10K_ITEM_1A:         SignalCategory.GOVERNANCE_SIGNALS,
    SourceType.SEC_10K_ITEM_7:          SignalCategory.LEADERSHIP_SIGNALS,
    SourceType.JOB_POSTING_LINKEDIN:    SignalCategory.TECHNOLOGY_HIRING,
    SourceType.JOB_POSTING_INDEED:      SignalCategory.TECHNOLOGY_HIRING,
    SourceType.PATENT_USPTO:            SignalCategory.INNOVATION_ACTIVITY,
    SourceType.PRESS_RELEASE:           SignalCategory.INNOVATION_ACTIVITY,
    SourceType.GLASSDOOR_REVIEW:        SignalCategory.CULTURE_SIGNALS,
    SourceType.BOARD_PROXY_DEF14A:      SignalCategory.GOVERNANCE_SIGNALS,
}

# CS4 analyst note source types are not in the CS2 SourceType enum.
# They are stored as raw strings in ChromaDB metadata.
_CS4_SOURCE_TO_CATEGORY: Dict[str, SignalCategory] = {
    "analyst_interview": SignalCategory.LEADERSHIP_SIGNALS,
    "dd_data_room":      SignalCategory.DIGITAL_PRESENCE,
}


# ---------------------------------------------------------------------------
# DimensionMapper
# ---------------------------------------------------------------------------

class DimensionMapper:
    """
    Maps CS2 SignalCategory (and SourceType) to CS3 Dimension weights.

    Primary entry points:
        get_dimension_weights(category)     → {Dimension: weight}
        get_primary_dimension(category)     → Dimension
        source_type_to_category(source)     → SignalCategory
        get_all_dimensions_for_evidence()   → [(Dimension, weight)] sorted desc
    """

    def get_dimension_weights(
        self, category: SignalCategory
    ) -> Dict[Dimension, float]:
        """
        Return the full dimension weight breakdown for a signal category.
        Returns an empty dict for unrecognised categories.
        """
        entry = _CATEGORY_WEIGHTS.get(category)
        if entry is None:
            return {}
        _, weights = entry
        return dict(weights)

    def get_primary_dimension(
        self, category: SignalCategory
    ) -> Dimension:
        """
        Return the highest-weight dimension for a signal category.
        Falls back to DATA_INFRASTRUCTURE for unrecognised categories so
        callers can always call .value on the result without a None check.
        """
        entry = _CATEGORY_WEIGHTS.get(category)
        if entry is None:
            return Dimension.DATA_INFRASTRUCTURE
        primary, _ = entry
        return primary

    def get_all_dimensions_for_evidence(
        self, category: SignalCategory
    ) -> List[Tuple[Dimension, float]]:
        """
        Return all (Dimension, weight) pairs sorted by weight descending.
        Useful when evidence contributes to multiple dimensions.
        """
        weights = self.get_dimension_weights(category)
        return sorted(weights.items(), key=lambda x: x[1], reverse=True)

    def source_type_to_category(
        self, source_type: SourceType | str
    ) -> SignalCategory | None:
        """
        Resolve a SourceType (or raw string for CS4 analyst notes) to its SignalCategory.
        Returns None for source types with no defined mapping.
        """
        if isinstance(source_type, str):
            return _CS4_SOURCE_TO_CATEGORY.get(source_type) or _SOURCE_TO_CATEGORY.get(source_type)  # type: ignore[arg-type]
        return _SOURCE_TO_CATEGORY.get(source_type) or _CS4_SOURCE_TO_CATEGORY.get(source_type.value)