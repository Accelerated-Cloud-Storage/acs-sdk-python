from setuptools import setup, find_packages

setup(
    name="acs",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "grpcio>=1.70.0",
        "grpcio-tools>=1.70.0",
        "PyYAML>=6.0.2",
        "protobuf>=5.29.3",
        "iniconfig>=2.1.1",
        "pluggy>=1.5.0",
        "protobuf>=5.29.3",
        "setuptools>=75.8.0",
    ],
    extras_require={
        'dev': [
            'pytest>=6.0',
            'black>=22.0',
            'mypy>=0.900',
            'grpc-stubs>=1.53.0',
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)
