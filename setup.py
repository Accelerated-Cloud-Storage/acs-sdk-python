from setuptools import setup, find_packages

setup(
    name="acs-sdk",
    version="0.4.0",
    packages=find_packages(),
    package_data={
        'acs_sdk': ['internal/ca-chain.pem'],
    },
    install_requires=[
        "grpcio>=1.70.0",
        "grpcio-tools>=1.70.0",
        "PyYAML>=6.0.2",
        "protobuf>=5.29.3",
        "iniconfig>=2.0.0",
        "pluggy>=1.5.0",
        "setuptools>=75.8.0",
        "fusepy>=3.0.1",
        "requests>=2.32.3",
        "typing_extensions>=4.12.2",
    ],
    extras_require={
        'dev': [
            'pytest>=8.3.5',
            'black>=23.12.1',
            'mypy>=0.900',
            'grpc-stubs>=1.53.0',
            'pylint>=3.3.4',
            'isort>=6.0.1',
        ]
    },
    python_requires='>=3.8',
    author="AcceleratedCloudStorage",
    author_email="sales@acceleratedcloudstorage.com",
    description="Python SDK for Accelerated Cloud Storage service",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/AcceleratedCloudStorage/acs-sdk-python",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Mozilla Public License Version 2.0",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)
