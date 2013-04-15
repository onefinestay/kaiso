import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
make_abs = lambda fn: os.path.join(here, fn)


setup(
    name='orp',
    packages=find_packages(exclude=['test', 'test.*']),
    version='0.0.1',
    author='onefinestay',
    author_email='engineering@onefinestay.com',
    url='https://github.com/onefinestay/',  # TODO
    install_requires=[
        'py2neo==1.5',
    ],
    tests_require=[],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Topic :: Software Development",
        "Topic :: Utilities",
        "Environment :: Console",
    ],
    description='Instance and release management made easy',
    # long_description=open(make_abs('README.rst')).read(),  # TODO
    include_package_data=True,
    zip_safe=False,
)
