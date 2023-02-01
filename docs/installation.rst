Installation
============


Supported Python versions
-------------------------

While we suggest using the latest Python version, :code:`enlyze` currently
supports Python 3.10 and newer.


On virtual environments
-------------------------

We highly encourage using virtual environments to manage your project's dependencies
for both development and production. This way, you keep your projects' dependencies
installed isolated and right next to your project.

As the number of projects you are working on  keeps growing, you might end up with
conflicting dependency versions, which can break one of your projects unnoticed and
in an unexpected manner.

Hence, be smart and start using virtual environment right from the beginning 🏃

👉 For an in-depth guide, please consult the `venv docs <https://docs.python.org/3/library/venv.html>`_.


.. _install-create-env:

Create a virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a project folder and create a virtualenv:

.. tabs::

   .. group-tab:: macOS/Linux

      .. code-block:: text

         $ mkdir myproject
         $ cd myproject
         $ python3 -m venv venv

   .. group-tab:: Windows

      .. code-block:: text

         > mkdir myproject
         > cd myproject
         > py -3 -m venv venv


.. _install-activate-env:

Activate the environment
~~~~~~~~~~~~~~~~~~~~~~~~

Before you work on your project, activate the corresponding environment:

.. tabs::

   .. group-tab:: macOS/Linux

      .. code-block:: text

         $ . venv/bin/activate

   .. group-tab:: Windows

      .. code-block:: text

         > venv\Scripts\activate

Your shell prompt will change to show the name of the activated
environment.


Install the SDK
---------------

Within the activated environment, use the following command to install :code:`enlyze`:

.. code-block:: sh

    $ pip install enlyze

🎉 You have successfully installed the ENLYZE Python SDK. Check out the :doc:`/quickstart` or go to the
:doc:`Documentation Overview </index>`.
