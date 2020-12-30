from os import environ


def set_version():
    version = environ.get('GITHUB_REF')
    assert version, 'GITHUB_REF is not defined'

    version = version.replace('refs-tags-v', '')

    lines = []
    init_py = 'storey/__init__.py'
    with open(init_py) as fp:
        for line in fp:
            if '__version__' in line:
                line = "__version__ = '{}'\n".format(version)
            lines.append(line)

    with open(init_py, 'w') as out:
        out.write(''.join(lines))


set_version()
