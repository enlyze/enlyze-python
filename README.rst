ENLYZE Python SDK
=================

Python SDK for interacting with the `ENLYZE platform <https://www.enlyze.com>`_

Getting started with development
--------------------------------

We use `tox <https://tox.wiki/en/latest/>`_ to manage most of the tasks in a development
workflow.

To start developing locally, we suggest that you have tox installed in your environment
(you can install it globally or in a virtualenv) and then you can run the following
command to create a development `virtualenv` for the project with the required
dependencies installed.

.. code-block:: console

    $ tox --devenv .venv -e dev

which will create a virtualenv for you under the `.venv` directory. Afterwards you can
activate the virtualenv and start hacking away

.. code-block:: console

    $ source .venv/bin/activate

Running the test suite
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ tox

Running the linters
~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ tox p -e flake8,black,isort,bandit,tox-fmt,pyproject-fmt

Running `mypy`
~~~~~~~~~~~~~~

.. code-block:: console

    $ tox -e mypy

Generating the documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ tox -e docs

Note that this requires the `Enchant C library
<https://pyenchant.github.io/pyenchant/install.html>` to be installed.

*Note for M1 users, the* ``Enchant`` *library doesn't work out of the box and the
workaround in the official docs is brittle, I suggest installing the library from*
``homebrew`` *and setting the environment variable* ``PYENCHANT_LIBRARY_PATH`` *like
so:*

.. code-block:: console

    $ export PYENCHANT_LIBRARY_PATH=/opt/homebrew/lib/libenchant-2.dylib
