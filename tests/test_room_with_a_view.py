#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `room_with_a_view` package."""

import pytest

from room_with_a_view import room_with_a_view


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


def test_command_line_interface():
    """Test the CLI."""
    command = room_with_a_view.RoomWithAViewCommand()

    # Need an 'action' argument on the command-line.
    with pytest.raises(SystemExit) as exc:
        command.handle()
