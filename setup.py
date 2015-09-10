from setuptools import setup, find_packages
setup(
    name = "pullsync",
    version = "0.0.2",
    packages = find_packages(),
    install_requires=[
        'cement',
        'httplib2>=0.9.1',
        'google-api-python-client',
        'hiredis',
        'python-coveralls',
        'python-dateutil',
        'python-Levenshtein',
        'pyxdg',
        'redis',
        'transmissionrpc',
    ],
    entry_points={
        'console_scripts': [
            'pullsync=pullsync.app:run',
        ],
    } 
)
