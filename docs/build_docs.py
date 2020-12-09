#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import argparse
import fnmatch
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from tabulate import tabulate

from docs.exts.docs_build import dev_index_generator, lint_checks  # pylint: disable=no-name-in-module
from docs.exts.docs_build.docs_builder import (  # pylint: disable=no-name-in-module
    DOCS_DIR,
    AirflowDocsBuilder,
    get_available_packages,
)
from docs.exts.docs_build.errors import (  # pylint: disable=no-name-in-module
    DocBuildError,
    display_errors_summary,
)
from docs.exts.docs_build.github_action_utils import with_group  # pylint: disable=no-name-in-module
from docs.exts.docs_build.spelling_checks import (  # pylint: disable=no-name-in-module
    SpellingError,
    display_spelling_error_summary,
)

if __name__ != "__main__":
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the ./build_docs.py command"
    )

CHANNEL_INVITATION = """\
If you need help, write to #documentation channel on Airflow's Slack.
Channel link: https://apache-airflow.slack.com/archives/CJ1LVREHX
Invitation link: https://s.apache.org/airflow-slack\
"""

ERRORS_ELIGIBLE_TO_REBUILD = [
    'failed to reach any of the inventories with the following issues',
    'undefined label:',
    'unknown document:',
]


def _get_parser():
    available_packages_list = " * " + "\n * ".join(get_available_packages())
    parser = argparse.ArgumentParser(
        description='Builds documentation and runs spell checking',
        epilog=f"List of supported documentation packages:\n{available_packages_list}" "",
    )
    parser.formatter_class = argparse.RawTextHelpFormatter
    parser.add_argument(
        '--disable-checks', dest='disable_checks', action='store_true', help='Disables extra checks'
    )
    parser.add_argument(
        "--package-filter",
        action="append",
        help=(
            "Filter specifying for which packages the documentation is to be built. Wildcard are supported."
        ),
    )
    parser.add_argument('--docs-only', dest='docs_only', action='store_true', help='Only build documentation')
    parser.add_argument(
        '--spellcheck-only', dest='spellcheck_only', action='store_true', help='Only perform spellchecking'
    )
    parser.add_argument(
        '--for-production',
        dest='for_production',
        action='store_true',
        help=('Builds documentation for official release i.e. all links point to stable version'),
    )

    return parser


def build_docs_for_packages(
    current_packages: List[str], docs_only: bool, spellcheck_only: bool, for_production: bool
) -> Tuple[Dict[str, List[DocBuildError]], Dict[str, List[SpellingError]]]:
    """Builds documentation for single package and returns errors"""
    all_build_errors: Dict[str, List[DocBuildError]] = defaultdict(list)
    all_spelling_errors: Dict[str, List[SpellingError]] = defaultdict(list)
    for package_name in current_packages:
        print("#" * 20, package_name, "#" * 20)
        builder = AirflowDocsBuilder(package_name=package_name, for_production=for_production)
        builder.clean_files()
        if not docs_only:
            spelling_errors = builder.check_spelling()
            if spelling_errors:
                all_spelling_errors[package_name].extend(spelling_errors)

        if not spellcheck_only:
            docs_errors = builder.build_sphinx_docs()
            if docs_errors:
                all_build_errors[package_name].extend(docs_errors)

    return all_build_errors, all_spelling_errors


def display_packages_summary(
    build_errors: Dict[str, List[DocBuildError]], spelling_errors: Dict[str, List[SpellingError]]
):
    """Displays a summary that contains information on the number of errors in each packages"""
    packages_names = {*build_errors.keys(), *spelling_errors.keys()}
    tabular_data = [
        {
            "Package name": package_name,
            "Count of doc build errors": len(build_errors.get(package_name, [])),
            "Count of spelling errors": len(spelling_errors.get(package_name, [])),
        }
        for package_name in sorted(packages_names, key=lambda k: k or '')
    ]
    print("#" * 20, "Packages errors summary", "#" * 20)
    print(tabulate(tabular_data=tabular_data, headers="keys"))
    print("#" * 50)


def print_build_errors_and_exit(
    message: str,
    build_errors: Dict[str, List[DocBuildError]],
    spelling_errors: Dict[str, List[SpellingError]],
) -> None:
    """Prints build errors and exists."""
    if build_errors or spelling_errors:
        if build_errors:
            display_errors_summary(build_errors)
            print()
        if spelling_errors:
            display_spelling_error_summary(spelling_errors)
            print()
        print(message)
        display_packages_summary(build_errors, spelling_errors)
        print()
        print(CHANNEL_INVITATION)
        sys.exit(1)


def main():
    """Main code"""
    args = _get_parser().parse_args()
    available_packages = get_available_packages()
    with with_group("Available packages"):
        for pkg in available_packages:
            print(f" - {pkg}")

    docs_only = args.docs_only
    spellcheck_only = args.spellcheck_only
    disable_checks = args.disable_checks
    package_filters = args.package_filter
    for_production = args.for_production

    print("Current package filters: ", package_filters)
    current_packages = (
        [p for p in available_packages if any(fnmatch.fnmatch(p, f) for f in package_filters)]
        if package_filters
        else available_packages
    )
    with with_group(f"Documentation will be built for {len(current_packages)} package(s)"):
        for pkg in current_packages:
            print(f" - {pkg}")

    all_build_errors: Dict[Optional[str], List[DocBuildError]] = {}
    all_spelling_errors: Dict[Optional[str], List[SpellingError]] = {}
    package_build_errors, package_spelling_errors = build_docs_for_packages(
        current_packages=current_packages,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
        for_production=for_production,
    )
    if package_build_errors:
        all_build_errors.update(package_build_errors)
    if package_spelling_errors:
        all_spelling_errors.update(package_spelling_errors)
    to_retry_packages = [
        package_name
        for package_name, errors in package_build_errors.items()
        if any(any((m in e.message) for m in ERRORS_ELIGIBLE_TO_REBUILD) for e in errors)
    ]
    if to_retry_packages:
        for package_name in to_retry_packages:
            if package_name in all_build_errors:
                del all_build_errors[package_name]
            if package_name in all_spelling_errors:
                del all_spelling_errors[package_name]

        package_build_errors, package_spelling_errors = build_docs_for_packages(
            current_packages=to_retry_packages,
            docs_only=docs_only,
            spellcheck_only=spellcheck_only,
            for_production=for_production,
        )
        if package_build_errors:
            all_build_errors.update(package_build_errors)
        if package_spelling_errors:
            all_spelling_errors.update(package_spelling_errors)

    if not disable_checks:
        general_errors = []
        general_errors.extend(lint_checks.check_guide_links_in_operator_descriptions())
        general_errors.extend(lint_checks.check_enforce_code_block())
        general_errors.extend(lint_checks.check_exampleinclude_for_example_dags())
        if general_errors:
            all_build_errors[None] = general_errors

    dev_index_generator.generate_index(f"{DOCS_DIR}/_build/index.html")
    print_build_errors_and_exit(
        "The documentation has errors.",
        all_build_errors,
        all_spelling_errors,
    )


main()
