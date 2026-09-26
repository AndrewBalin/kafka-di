from __future__ import annotations

import re
import tomllib
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = ROOT / 'pyproject.toml'
README_PATH = ROOT / 'README.md'
BADGES_PATTERN = re.compile(r'<!-- badges:start -->.*?<!-- badges:end -->', re.DOTALL)


def badge(label: str, value: str, color: str, logo: str) -> str:
    escaped_label = quote(label).replace('-', '--')
    escaped_value = quote(value).replace('-', '--')
    badge_url = (
        f'https://img.shields.io/badge/{escaped_label}-{escaped_value}-{color}?logo={quote(logo)}&logoColor=white'
    )
    return badge_url


def dependency_specifier(dependencies: list[str], package_name: str) -> str:
    for dependency in dependencies:
        if dependency.startswith(package_name):
            return dependency.removeprefix(package_name) or 'latest'
    raise ValueError(f'Missing required dependency: {package_name}')


def main() -> None:
    with PYPROJECT_PATH.open('rb') as pyproject_file:
        project = tomllib.load(pyproject_file)['project']

    package_name = project['name']
    package_version = project['version']
    python_version = project['requires-python']
    confluent_kafka_version = dependency_specifier(project['dependencies'], 'confluent-kafka')

    badges = '\n'.join(
        (
            '<!-- badges:start -->',
            f'[![{package_name}]({badge(package_name, package_version, "3776AB", "pypi")})]'
            f'(https://pypi.org/project/{package_name}/)',
            f'[![Python]({badge("python", python_version, "3776AB", "python")})](https://www.python.org/)',
            f'[![confluent-kafka]({badge("confluent-kafka", confluent_kafka_version, "231F20", "apachekafka")})]'
            '(https://pypi.org/project/confluent-kafka/)',
            '<!-- badges:end -->',
        )
    )

    readme = README_PATH.read_text()
    updated_readme, replacements = BADGES_PATTERN.subn(badges, readme)
    if replacements != 1:
        raise ValueError('README.md must contain exactly one badges section.')

    README_PATH.write_text(updated_readme)


if __name__ == '__main__':
    main()
