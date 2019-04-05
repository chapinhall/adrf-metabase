###############
ADRF Metabase
###############

Tools for handling metadata associated with administrative data sets.

--------------
Requirements
--------------

- PostgreSQL 9.5

- Python 3.5

- See requirements.txt (``pip install -r requirements.txt``)

-----------------------
Prepare the database
-----------------------

Create superuser ``metaadmin`` and store credentials in ``.pgpass`` file.

Grant ``metaadmin`` login privilege.

Create schema ``metabase``.

Sample codes::

    CREATE ROLE metaadmin WITH LOGIN SUPERUSER;

    CREATE SCHEMA metabase;

------------------------
Run migration script
------------------------

Currently there is only one version of the database. You can create all the
tables by running::

    alembic upgrade head

To revert the migration, run::

    alembic downgrade base

-----------
Run Tests
-----------

Tests require `testing.postgresql <https://github.com/tk0miya/testing.postgresql>`_.

``pip install testing.postgresql``

Run tests with the following command under the root directory of the project::

    pytest tests/

----------
Build docs
----------

Under the ``./docs/`` directory, run::

    sphinx-apidoc -o source/ ../metabase --force --separate

    make html
    
To build docs in PDF, first install the dependencies listed on the 
`LaTexBuilder documentation <http://www.sphinx-doc.org/en/master/usage/builders/index.html#sphinx.builders.latex.LaTeXBuilder>`_,
and then run ``make latexpdf``.
