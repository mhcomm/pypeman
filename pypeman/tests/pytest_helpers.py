"""
Helpers / fixtures for pytest
"""

import pytest

import pypeman.channels
import pypeman.endpoints
import pypeman.nodes


@pytest.fixture(scope="function")
def clear_graph():
    """
    ensure, that before and after a pypeman test
    the pypeman graph is entirely cleared
    """
    n_nodes = len(pypeman.nodes.all_nodes)
    n_channels = len(pypeman.channels.all_channels)
    n_endpoints = len(pypeman.endpoints.all_endpoints)
    pypeman.nodes.reset_pypeman_nodes()
    pypeman.channels.reset_pypeman_channels()
    pypeman.endpoints.reset_pypeman_endpoints()
    yield n_nodes, n_channels, n_endpoints
    pypeman.nodes.reset_pypeman_nodes()
    pypeman.channels.reset_pypeman_channels()
    pypeman.endpoints.reset_pypeman_endpoints()
