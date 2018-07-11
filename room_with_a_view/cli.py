# -*- coding: utf-8 -*-

"""Console script for room_with_a_view."""


import sys
from room_with_a_view.room_with_a_view import RoomWithAViewCommand


def main():
    RoomWithAViewCommand().handle()


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
