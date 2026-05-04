"""Audit trail module providing tamper-evident event logging.

Hash chain tracking and verification for end-to-end auditability.
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
