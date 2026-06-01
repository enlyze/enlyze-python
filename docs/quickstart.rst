Quickstart
==========

Make sure you have the ``enlyze`` SDK Python package :doc:`installed </installation>`.

Authentication
--------------

In order to interact with the ENLYZE platform you need to acquire an API key. You can
create and manage API keys yourself directly within the ENLYZE platform. See
`Creating and Managing API Keys <https://docs.enlyze.com/en/administration/api-keys>`_
for step-by-step instructions on how to retrieve one.

Every API key grants full access to all the data of your organization, so please
keep it safe and treat it like a password! API keys are only displayed once at creation
time and cannot be recovered. If you have lost your key or think it might have been
compromised, simply revoke it and create a new one.

Client setup
------------

The ``EnlyzeClient`` class is your main entrypoint to interact with the ENLYZE platform.
It takes care of authentication so you must pass your API key to it.

.. code-block:: pycon

    >>> from enlyze import EnlyzeClient
    >>> enlyze = EnlyzeClient('my_api_key')

Exploration
-----------

Now that you have an instance of ``EnlyzeClient`` you can explore your data, for example
checking which :ref:`sites <site>` are available.

.. code-block:: pycon

    >>> enlyze.get_sites()
    [Site(id=1, name='Köln', address='Heliosstrasse 6a, 50825 Köln')]
