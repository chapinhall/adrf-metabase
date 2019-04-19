###############
ADRF Metabase
###############

Tools for handling metadata associated with administrative data sets.

--------------
Requirements
--------------

- `PostgreSQL 9.5 <https://www.postgresql.org/download/>`_
- Python 3.5
- Prerequisite packages can be installed with::

    pip install -r requirements.txt

-----------------------
Preparing the database
-----------------------

Metabase writes metadata to a ``metabase`` schema as superuser ``metaadmin``. These can be configured in `metabase/settings.py <metabase/settings.py>`_. By default, we need to first create a superuser with login privilege and store its database credentials in a `pgpass <https://www.postgresql.org/docs/9.5/libpq-pgpass.html>`_ file.

The sample SQL codes below create superuser ``metaadmin`` and schema ``metabase``::

    CREATE ROLE metaadmin WITH LOGIN SUPERUSER;
    SET ROLE metaadmin;
    CREATE SCHEMA metabase;

To initiate metabase tables under the ``metabase`` schema, run an `Alembic <https://alembic.sqlalchemy.org/en/latest/>`_ migration with the following shell command::

    alembic upgrade head

It runs the migration scripts under the `<alembic/>`_ folder.

Now the Metabase is ready to host metadata.

--------------------
Command line usage
--------------------

Once the ``metabase`` schema and tables have been set up, the metadata extraction process can be initiated with ::

    python extract.py -s <schema_name> -t <table_name>

Replace ``<schema_name>`` and ``<table_name>`` with the name of the postgres schema and table you want to extract metadata from. 

Alternatively, ``extract.py`` can take the path of a JSON configuration file as a command line argument::

    python extract.py -f <config.json>

The config file contains the following key/value pairs::

    {  
        "schema": "",
        "table": "",
        "categorical_threshold": 2,
        "date_format": "",
        "type_overrides":{  
            "column_name_1": "type_1",
            "column_name_2": "type_2",
        },
        "gmeta_output": "exported_gmeta.json"
    }

- ``schema`` and ``table`` receive the name of the postgres schema and table that we want to extract metadata from
- ``categorical_threshold`` takes an integer. If the number of unique values in a column is less than or equal to this threshold, the column will be considered as categorical and its metadata will be processed accordingly.
  - Default to 10 if leave blank
- ``type_overrides`` takes column name / data type pairs. Data type should be one of the following: ``text``, ``code`` (categorical), ``numeric``, or ``date``. If the type of a column is specified here, Metabase will directly use it and bypass the type-detection process for that column.
- ``date_format`` takes a string representing the format of values in date columns.
- ``gmeta_output`` takes a string specifying the filepath for metadata output in JSON format (*Gmeta*).
  - If leave blank, will not export Gmeta.

-----------
Tests
-----------

We use `pytest <https://doc.pytest.org/>`_ for unit test and `testing.postgresql <https://github.com/tk0miya/testing.postgresql>`_ to setup testing databases. The former can be installed with ::

    pip install pytest

Note that the testing.postgresql 1.3.0 on PyPI has an `open issue <https://github.com/tk0miya/testing.postgresql/issues/16>`_ that can lead to false errors on Windows systems. It can be avoided by installing their master branch on GitHub via ::

    pip install git+https://github.com/tk0miya/testing.postgresql.git

As of April 9, 2019, its PyPI distribution works for Linux, but Linux users may also want to install from the master branch since it seems that that project is no longer active.

Tests can be run with the following command under the root directory of the project::

    pytest tests/

-------------
Documentation
-------------

Documentation of this project is built with `Sphinx <http://www.sphinx-doc.org/en/master/>`_, which can be installed with ::

    pip install sphinx

Also, an online build of the documentation is hosted by `Read the Docs <https://readthedocs.org/>`_ and can be found at https://adrf-metabase.readthedocs.io.


To build the documentation locally, first go to the `<docs/>`_ folder and run `sphinx-apidoc <https://www.sphinx-doc.org/en/master/man/sphinx-apidoc.html>`_ to generate/update ``rst`` files under `<docs/source/>`_. For example::

    sphinx-apidoc -o source/ ../metabase --force --separate

In the sample command above, ``-o source/`` specifies the output directory as ``source/``; ``../metabase`` is our module path; ``--force`` overwrites existing ``rst`` files; ``--separate`` puts documentation for each module on its own page.

Last, documentation can be rendered as HTML with ``make html`` or PDF with ``make latexpdf``.

Note that latexpdf has some prerequisites that may take some time (> 30 minutes) and space (several GBs) to install. Information about the dependencies can be found on the `LaTexBuilder documentation <http://www.sphinx-doc.org/en/master/usage/builders/index.html#sphinx.builders.latex.LaTeXBuilder>`_ for Linux and `TeX Live <https://tug.org/texlive/windows.html>`_ for Windows.

The outputs can be found under `<docs/build/>`_.
