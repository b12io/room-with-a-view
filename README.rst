================
Room with a View
================


.. image:: https://img.shields.io/pypi/v/room_with_a_view.svg
        :target: https://pypi.python.org/pypi/room_with_a_view

.. image:: https://img.shields.io/travis/b12io/room_with_a_view.svg
        :target: https://travis-ci.org/b12io/room_with_a_view

.. image:: https://readthedocs.org/projects/room-with-a-view/badge/?version=latest
        :target: https://room-with-a-view.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/marcua/room_with_a_view/shield.svg
     :target: https://pyup.io/repos/github/marcua/room_with_a_view/
     :alt: Updates



View management for Amazon's Redshift


* Free software: Apache Software License 2.0
* Documentation: https://room-with-a-view.readthedocs.io.

Features
--------

Room with a view is a python script that automatically parses a collection of
SQL files to find view and function definitions and their dependencies. It then
makes it easy to sync those views with Redshift, automatically dropping and
recreating dependent views as necessary so that there are no errors. The key
benefits are:

* No writing code to send view SQL to Redshift: the script does it for you.
* Edit any view and sync it without worrying about needing to drop or recreate
  views that depend on it.
* Error handling tells you exactly where there are errors in your views.
* The script runs in an atomic transaction, so you can't accidentally enter a
  broken state by syncing some views and not others.


Setup
-----

* Install the package: ``pip install room_with_a_view``.

* Create ``settings.yaml``, and edit the file to configure your Redshift connection and the location of your .sql files. Example ``settings.yaml`` file:

.. code-block:: yaml

   connections:
     default:
       host: localhost
       port: 5432
       user: awsuser
       password: **CHANGEME**
       dbname: postgres

   directories:
     - .

* You're ready to go! Try ``room_with_a_view sync-all`` to sync all your views, or ``room_with_a_view --help`` to learn more about the command.

Usage
-----

::

    usage: room_with_a_view.py [-h]
                               [--view-names [VIEW-OR-FUNCTION-NAME [VIEW-OR-FUNCTION-NAME ...]]]
                               [--file-names [FILE-PATH [FILE-PATH ...]]]
                               [--connection CONNECTION]
                               [--settings SETTINGS] [--verbosity VERBOSITY]
                               {sync,drop-all,sync-all,list,drop}

    Manages Redshift SQL views. Possible actions:
        sync: Syncs specific views or functions (identified by the --view-names or --file-names parameters).
        drop-all: Drops all views and functions in all .sql files in a set of directories (identified by the --directories parameter). The directory will be searched recursively.
        sync-all: Syncs all views and functions in all .sql files in a set of directories (identified by the --directories parameter). The directory will be searched recursively.
        list: lists all known views and functions.
        drop: Drops specific views or functions (identified by the --view-names or --file-names parameters).

    positional arguments:
      {sync,drop-all,sync-all,list,drop}
                            The action to perform.

    optional arguments:
      -h, --help            show this help message and exit
      --view-names [VIEW-OR-FUNCTION-NAME [VIEW-OR-FUNCTION-NAME ...]]
                            Names of views or functions to which to apply the action.
      --file-names [FILE-PATH [FILE-PATH ...]]
                            Paths to .sql files to which to apply the action.
      --connection CONNECTION
                            Name of the Redshift connection to use (or "default",
                            if not specified). The name must match a connection in
                            settings.yaml
      --settings SETTINGS   Location of the settings file (settings.yaml by
                            default)
      --verbosity VERBOSITY
                            Verbosity of script output. 0 will output nothing, 1
                            will output names of views and functions being dropped
                            and created, and 2 will output all executed sql

Examples
--------

* ``room_with_a_view.py sync-all``: Syncs all views and functions in all SQL files in the default directory specified in ``settings.yaml``. Drops and recreates existing views, and makes sure views are created in dependency order.

* ``room_with_a_view.py sync --view-names my_view1 my_func1 --file-names ../sql/my_file.sql``: Syncs the specific view ``my_view1`` and function ``my_func1``, as well as all views and functions in the file ``../sql/my_file.sql``.

* ``room_with_a_view.py drop-all --connection other_connection``: Drops all views and functions in the default directory, using the connection info specified in ``settings.yaml`` under the name ``other_connection`` to connect to Redshift.

* ``room_with_a_view.py drop --view-names my_view1 --directories other_dir1 other_dir2 --settings /path/to/fancy_settings.yaml``: Drops the view ``my_view1``, looking for SQL files that contain the view and its dependents in the directories specified by ``other_dir1`` and ``other_dir2`` in the settings file located in ``/path/to/fancy_settings.yaml``.

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
