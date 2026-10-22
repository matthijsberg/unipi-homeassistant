#!/usr/bin/env bash
# Export/import the machine-specific files of one Unipi as a private archive. See tools/site_bundle.py.
exec python3 "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/site_bundle.py" "$@"
