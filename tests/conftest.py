import pytest
import os

def pytest_configure(config):
    """Configure test environment."""
    # Set default profile for tests
    os.environ.setdefault("ACS_PROFILE", "default")

def pytest_sessionstart(session):
    """Called before test session starts."""
    print("\nSetting up test session...")

def pytest_sessionfinish(session, exitstatus):
    """Called after test session finishes."""
    print("\nTearing down test session...")
