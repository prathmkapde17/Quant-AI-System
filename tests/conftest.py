"""Shared test fixtures and configuration."""

import pytest

from src.core.config import load_settings, reset_settings


@pytest.fixture(autouse=True)
def _reset_settings_singleton():
    """Reset settings singleton before each test to avoid cross-contamination."""
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def settings():
    """Load settings from default config for testing."""
    return load_settings()
