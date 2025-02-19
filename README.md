# ACS SDK for Python
The Python SDK for Accelerated Cloud Storage's Object Storage offering. 

`acs-sdk-python` is the ACS SDK for the Python programming language.

The SDK requires a minimum version of Python 3.8.

Check out the [Release Notes] for information about the latest bug fixes, updates, and features added to the SDK.

Jump To:
* [Getting Started](#getting-started)
* [Getting Help](#getting-help)

## Getting started
[![SDK Documentation](https://img.shields.io/badge/SDK-Documentation-blue)](https://acceleratedcloudstorage.com) [![API Reference](https://img.shields.io/badge/API-Reference-blue.svg)]() [![Demo](https://img.shields.io/badge/Demo-Videos-blue.svg)](https://www.youtube.com/@AcceleratedCloudStorageSales) 

#### Setup credientials 
Downloading your credentials from the console on the [website](https://acceleratedcloudstorage.com).

Next, set up credentials (in e.g. ``~/.acs/credentials``):
```
default:
    access_key_id = YOUR_KEY
    secret_access_key = YOUR_SECRET
```
Note: You can include multiple profiles and set them using the ACS_PROFILE environment variable. 

#### Initialize project
Assuming that you have a supported version of Python installed, you can first set up your environment with:
```python
python -m venv .venv
source .venv/bin/activate
```
Then, you can install acs from PyPI with:
```python
python -m pip install acs
```
Or you can install it from source (preferred)
```
$ git clone https://github.com/AcceleratedCloudStorage/acs-sdk-python
$ python -m pip install -r requirements.txt
$ python -m pip install -e .
```
#### Write Code
Check out the example folder. 

## Getting Help

Please use these community resources for getting help. 

### Feedback

If you encounter a bug with the ACS SDK for Python we would like to hear about it.
Search the [existing issues][Issues] and see if others are also experiencing the same issue before opening a new issue. Please include the version of ACS SDK for Python, Python language, and OS you’re using. Please also include reproduction case when appropriate. Keeping the list of open issues lean will help us respond in a timely manner.

### Discussion  

We have a discussion forum where you can read about announcements, product ideas, partcipate in Q&A. Here is a link to the [discussion].

### Contact us 

Email us at sales@acceleratedcloudstorage.com if there is a pressing issue for you. 

[Issues]: https://github.com/AcceleratedCloudStorage/acs-sdk-python/issues
[Discussion]: https://github.com/AcceleratedCloudStorage/acs-sdk-python/discussions
[Release Notes]: https://github.com/AcceleratedCloudStorage/acs-sdk-python/blob/main/CHANGELOG.md