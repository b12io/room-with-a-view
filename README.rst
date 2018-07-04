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
SQL files to find View and Function definitions and their dependencies. It then
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

* Copy ``settings.yaml.default`` to ``settings.yaml``, and edit the file to configure your Redshift connection and the location of your .sql files. Example ``settings.yaml`` file:

.. code-block:: yaml

   connections:
     default:
       host: localhost
       port: 5432
       user: awsuser
       password: **CHANGEME**
       dbname: postgres

   directories:
     default: .

* Install requirements with ``pip install -r requirements.txt``

Usage
-----

::

    usage: room_with_a_view.py [-h] [--view-names [VIEW-NAME [VIEW-NAME ...]]]
                               [--file-names [FILE-PATH [FILE-PATH ...]]]
                               [--connection CONNECTION]
                               [--directories [DIRECTORY [DIRECTORY ...]]]
                               [--settings SETTINGS]
                               {sync-all,sync,drop,drop-all}

    Manages Redshift SQL views. Possible actions:
	    sync-all: Syncs all views in all .sql files in a set of directories (identified by the --directories parameter). The directory will be searched recursively
	    sync: Syncs specific views (identified by the --view-names or --file-names parameters)
	    drop: Drops specific views (identified by the --view-names or --file-names parameters)
	    drop-all: Drops all views in all .sql files in a set of directories (identified by the --directories parameter). The directory will be searched recursively

    positional arguments:
      {sync-all,sync,drop,drop-all}
                            The action to perform.

    optional arguments:
      -h, --help            show this help message and exit
      --view-names [VIEW-NAME [VIEW-NAME ...]]
                            View names to manage.
      --file-names [FILE-PATH [FILE-PATH ...]]
                            Paths to .sql files to manage.
      --connection CONNECTION
                            Name of the Redshift connection to use (or "default",
                            if not specified). The name must match a connection in
                            settings.yaml
      --directories [DIRECTORY [DIRECTORY ...]]
                            Directory names to search for SQL files (or "default"
                            if not specified). Names must match directories in
                            settings.yaml
      --settings SETTINGS   Location of the settings file (settings.yaml by
                            default)

Examples
--------

* ``room_with_a_view.py sync-all``: Syncs all views in all SQL files in the default directory specified in ``settings.yaml``. Drops and recreates existing views, and makes sure views are created in dependency order.

* ``room_with_a_view.py sync --view-names my_view1 my_view2 --file-names ../sql/my_file.sql``: Syncs the specific views ``my_view1`` and ``my_view2``, as well as all views in the file ``../sql/my_file.sql``.

* ``room_with_a_view.py drop-all --connection other_connection``: Drops all views in the default directory, using the connection info specified in ``settings.yaml`` under the name ``other_connection`` to connect to Redshift.

* ``room_with_a_view.py drop --view-names my_view1 --directories other_dir1 other_dir2 --settings /path/to/fancy_settings.yaml``: Drops the view ``my_view1``, looking for SQL files that contain the view and its dependents in the directories specified by ``other_dir1`` and ``other_dir2`` in the settings file located in ``/path/to/fancy_settings.yaml``.

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
