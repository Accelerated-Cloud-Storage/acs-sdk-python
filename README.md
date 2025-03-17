# ACS SDK for Python
The Python SDK for Accelerated Cloud Storage's Object Storage offering. 

`acs-sdk-python` is the ACS SDK for the Python programming language.

The SDK requires a minimum version of Python 3.9.

Check out the [Release Notes] for information about the latest bug fixes, updates, and features added to the SDK.

Jump To:
* [Getting Started](#getting-started)
* [Getting Help](#getting-help)

### Python version support policy

The SDK follows a release policy of an additional six months of support for the most recently deprecated language version.

**ACS reserves the right to drop support for unsupported Python versions earlier to
address critical security issues.**

## Getting started
[![Website](https://img.shields.io/badge/Website-Console-blue)](https://acceleratedcloudstorage.io) [![Python](https://img.shields.io/badge/pypi-blue)](https://pypi.org/project/acs-sdk) [![API Reference](https://img.shields.io/badge/API-Reference-blue.svg)](https://github.com/AcceleratedCloudStorage/acs-sdk-python/blob/main/docs/API.md) [![Demo](https://img.shields.io/badge/Demo-Videos-blue.svg)](https://www.youtube.com/@AcceleratedCloudStorageSales)

#### Get credentials

Get your credentials and setup payments from the console on the [website](https://acceleratedcloudstorage.io).

Next, set up credentials (in e.g. ``~/.acs/credentials``):

```
default:
    access_key_id = YOUR_KEY
    secret_access_key = YOUR_SECRET
```

Note: You can include multiple profiles and set them using the ACS_PROFILE environment variable. See the examples/config folder for a sample file.

#### Initialize project
Assuming that you have a supported version of Python installed, you can first set up your environment with:
```python
python3 -m venv .venv
source .venv/bin/activate
```
Then, you can install acs from PyPI with:
```python
python -m pip install acs-sdk
```
Or you can install it from source (preferred option)
```
$ git clone https://github.com/AcceleratedCloudStorage/acs-sdk-python
$ python -m pip install -r requirements.txt
$ python -m pip install -e .
```
#### Write Code
You can either use the client for an interface similar to the AWS SDK or a FUSE mount for a file system interface. Check out the example folder or the docs folder for more details. 

## Share bucket 
You can also bring your existing buckets into the service by setting a bucket policy and then sharing the bucket with the service. 

### Step 1: Setting a bucket policy
Here is the AWS reference guide for [bucket policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/add-bucket-policy.html). You can set the following bucket policy through the AWS Console or SDK to enable ACS to access it. 
```
{
"Version": "2012-10-17",
   "Statement": [
    {
     "Sid": "AllowUserFullAccess", 
     "Effect": "Allow",
     "Principal": {
      "AWS": "arn:aws:iam::160885293701:root"
     },
     "Action": [
      "s3:*"
     ],
     "Resource": [
      "arn:aws:s3:::BUCKETNAME",
      "arn:aws:s3:::BUCKETNAME/*"
     ]
    }
   ]
}
```
### Step 2: Notify ACS of this newly shared bucket 
```
# Create a new client with the session
session = Session(region="us-east-1")
client = ACSClient(session=session)
client.share_bucket("BUCKETNAME")
```

## Getting Help

Please use these community resources for getting help. 

### Feedback

If you encounter a bug with the ACS SDK for Python we would like to hear about it.
Search the [existing issues][Issues] and see if others are also experiencing the same issue before opening a new issue. Please include the version of ACS SDK for Python, Python language, and OS you’re using. Please also include reproduction case when appropriate. Keeping the list of open issues lean will help us respond in a timely manner.

### Discussion  

We have a discussion forum where you can read about announcements, product ideas, partcipate in Q&A. Here is a link to the [discussion].

### Contact us 

Email us at sales@acceleratedcloudstorage.com if you have any further questions or concerns. 

[Issues]: https://github.com/AcceleratedCloudStorage/acs-sdk-python/issues
[Discussion]: https://github.com/AcceleratedCloudStorage/acs-sdk-python/discussions
[Release Notes]: https://github.com/AcceleratedCloudStorage/acs-sdk-python/blob/main/CHANGELOG.md