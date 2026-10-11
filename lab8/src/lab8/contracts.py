"""Extend the Lab 5 contract with the key required by Lab 6 Gold."""
from lab5.contracts import WIKI_RULES

RULES = {**WIKI_RULES, "valid_namespace": "namespace IS NOT NULL"}


def bootstrap_mode(existing, expected, has_successful_release=False):
    """Recover interrupted first builds; reject missing tables after a release."""
    if has_successful_release and set(existing) != set(expected):
        raise ValueError("A released Gold table is missing; investigate before refreshing")
    return not has_successful_release


def check_delivery_counts(bronze, silver, quarantine, unique_deliveries):
    if bronze != unique_deliveries:
        raise ValueError("Duplicate Kafka topic/partition/offset in Bronze")
    if bronze != silver + quarantine:
        raise ValueError("Bronze does not reconcile to Silver plus quarantine")
    if silver == 0:
        raise ValueError("Wikipedia Silver is empty")
