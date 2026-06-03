"""
Shared pytest fixtures for all tests.

These fixtures are automatically available to all test modules in the package.
"""

import os

import pytest


@pytest.fixture(autouse=True, scope="function")
def default_test_mission(monkeypatch):
    """
    Automatically set HERMES as the default mission for all tests.

    This fixture runs automatically before each test function and ensures
    that tests have a consistent mission configuration (HERMES) unless
    explicitly overridden by the test itself or the use_mission fixture.

    The autouse=True makes this fixture apply to all tests without explicit declaration.
    The monkeypatch ensures environment changes are cleaned up after each test.

    Note: This does NOT affect doctests. Doctests must explicitly set the
    mission in their example code if they need a specific mission configuration.
    """
    import swxsoc
    from sdc_aws_utils.config import _reconfigure_globals

    # Only set if not already set (allows tests to override)
    if "SWXSOC_MISSION" not in os.environ:
        monkeypatch.setenv("SWXSOC_MISSION", "hermes")
        swxsoc._reconfigure()
        # Re-read module-level globals in config.py so they reflect the new mission
        _reconfigure_globals()


@pytest.fixture(scope="function")
def use_mission(request, monkeypatch):
    """
    Fixture to explicitly set a mission for a test function.

    This fixture allows tests to specify which mission configuration to use
    via indirect parametrization. It overrides the default_test_mission fixture.

    Parameters
    ----------
    request : pytest.Request
        The pytest request object containing the parameter for the mission.
    monkeypatch : pytest.MonkeyPatch
        The pytest monkeypatch fixture for environment modification.

    Yields
    ------
    str
        The name of the mission that was configured for the test.

    Examples
    --------
    Single mission test::

        @pytest.mark.parametrize('use_mission', ['padre'], indirect=True)
        def test_with_padre(use_mission):
            # Test runs with PADRE mission config
            assert swxsoc.config['mission']['mission_name'] == 'padre'

    Multiple missions::

        @pytest.mark.parametrize('use_mission', ['hermes', 'padre', 'swxsoc'], indirect=True)
        def test_all_missions(use_mission):
            # Test runs three times, once for each mission
            assert swxsoc.config['mission']['mission_name'] == use_mission
    """
    import metatracker
    import swxsoc
    from sdc_aws_utils.config import _reconfigure_globals

    mission = request.param if hasattr(request, "param") else "hermes"
    monkeypatch.setenv("SWXSOC_MISSION", mission)
    swxsoc._reconfigure()
    _reconfigure_globals()
    metatracker.set_config()
    yield mission
    # Explicitly reconfigure back to default after test completes
    # This is necessary because swxsoc.config is module-level state
    # that persists across tests in the same process
    # This ensures the config is reset even if monkeypatch cleanup hasn't run yet
    monkeypatch.setenv("SWXSOC_MISSION", "hermes")
    swxsoc._reconfigure()
    _reconfigure_globals()
    metatracker.set_config()
