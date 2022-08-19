from setuptools import setup, find_packages


def version():
    with open('storey/__init__.py') as fp:
        for line in fp:
            if line.startswith('__version__'):
                _, version = line.split('=')
                return version.replace("'", '').strip()


def load_deps(file_name):
    """Load dependencies from requirements file"""
    deps = []
    with open(file_name) as fp:
        for line in fp:
            line = line.strip()
            if not line or line[0] == '#':
                continue
            deps.append(line)
    return deps


install_requires = load_deps('requirements.txt')
tests_require = load_deps('dev-requirements.txt')
extras_require = {
    "s3": ["s3fs~=0.5"],
    "az": ["adlfs~=0.5"],
    "kafka": ["kafka-python~=2.0"],
    "redis": ["redis~=4.3"],
    "lupa": ["lupa~=1.13"],
}


with open('README.md') as fp:
    long_desc = fp.read()

setup(
    name='storey',
    version=version(),
    description='Async flows',
    long_description=long_desc,
    long_description_content_type='text/markdown',
    author='Iguazio',
    author_email='yaronh@iguazio.com',
    license='Apache',
    url='https://github.com/mlrun/storey',
    packages=find_packages(),
    install_requires=install_requires,
    extras_require=extras_require,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
    ],
    tests_require=tests_require,
)
