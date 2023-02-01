Quickstart
==========

Make sure you have the ``enlyze`` SDK Python package :doc:`installed </installation>`.

Authentication
--------------

In order to interact with the ENLYZE platform you need to acquire an API token. If you
haven't received one already, please reach out to us at hello@enlyze.com. This token
will give you access to all the data of your organization, so please keep it safe! In
case you have lost your token or you think it might have been compromised please reach
out to us as well.

Client setup
------------

The ``EnlyzeClient`` class is your main entrypoint to interact with the ENLYZE platform.
It takes care of authentication so you must pass your access token to it.

.. code-block:: pycon

    >>> from enlyze import EnlyzeClient
    >>> enlyze = EnlyzeClient('my_api_token')

Exploration
-----------

Now that you have an instance of ``EnlyzeClient`` you can explore your data, for example
checking which :ref:`sites <site>` are available.

.. code-block:: pycon

    >>> enlyze.get_sites()
    [Site(id=1, name='Köln', address='Heliosstrasse 6a, 50825 Köln')]
