"""Run acceptance scenarios explicitly against the pinned trial_2 environment."""

import argparse

import check_late_duplicate_dev
import check_negative_dev
import check_recovery_dev
import cleanup_negative_dev
import validate_dev
from _acceptance import AcceptanceEnvironment

SCENARIOS = {
    "validate": validate_dev.main,
    "negative": check_negative_dev.main,
    "recovery": check_recovery_dev.main,
    "late": check_late_duplicate_dev.main,
    "cleanup": cleanup_negative_dev.main,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=SCENARIOS)
    args, scenario_args = parser.parse_known_args(argv)
    SCENARIOS[args.scenario](scenario_args, environment=AcceptanceEnvironment("azure_trial_2"))


if __name__ == "__main__":
    main()
