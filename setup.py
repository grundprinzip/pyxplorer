from distutils.core import setup

setup(
    name='pyxplorer',
    version='0.1.0',
    author='Martin Grund',
    author_email='grundprinzip+piphgmail.com',
    packages=['pyxplorer'],
    url='http://github.com/grundprinzip/pyxplorer',
    license='LICENSE',
    description='Useful towel-related stuff.',
    long_description=open('README.md').read(),
    install_requires=[
        "snakebite",
        "pyhs2",
        "pandas"
    ],
)
