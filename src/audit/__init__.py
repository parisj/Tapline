"""Audit trail module for GxP compliance.

Provides hash chain tracking and verification for tamper-evident event logging.
"""

from src.audit.chain import (
    HashChainTracker,
    HashChainVerifier,
    SignatureProvider,
    verify_event_chain,
)

__all__ = [
    "HashChainTracker",
    "HashChainVerifier",
    "SignatureProvider",
    "verify_event_chain",
]
