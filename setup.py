# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from setuptools import find_packages, setup


def version():
    with open("storey/__init__.py") as fp:
        for line in fp:
            if line.startswith("__version__"):
                _, version = line.split("=")
                return version.replace('"', "").strip()


def load_deps(file_name):
    """Load dependencies from requirements file"""
    deps = []
    with open(file_name) as fp:
        for line in fp:
            line = line.strip()
            if not line or line[0] == "#":
                continue
            deps.append(line)
    return deps


install_requires = load_deps("requirements.txt")
tests_require = load_deps("dev-requirements.txt")
extras_require = {
    "s3": ["s3fs~=0.5"],
    "az": ["adlfs~=0.5"],
    "kafka": ["kafka-python~=2.0"],
    "redis": ["redis~=4.3"],
    "lupa": ["lupa~=1.13"],
    "sqlalchemy": ["sqlalchemy~=1.3"],
}


with open("README.md") as fp:
    long_desc = fp.read()

setup(
    name="storey",
    version=version(),
    description="Async flows",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    author="Iguazio",
    author_email="yaronh@iguazio.com",
    license="Apache",
    url="https://github.com/mlrun/storey",
    packages=find_packages(),
    install_requires=install_requires,
    extras_require=extras_require,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries",
    ],
    tests_require=tests_require,
)
