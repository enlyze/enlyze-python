Concepts
========

Within this document, we will discuss the central concepts of the ENLYZE platform and
understand their relation to one another.

.. _site:

Site
----

The concept of a *site* refers to a physical production site. Depending on its size,
your organization might have one or many of them. In the ENLYZE platform, each site has
a name and an address, which makes it easy to identify for humans.

.. _appliance:

Appliance
---------

An *appliance* refers to a machine that your organization uses to produce goods. For
example, a CNC-milling center, a blown film extrusion line or an injection molding
machine all represent an appliance in the ENLYZE platform. Just as a physical machine is
located at one production site, an appliance in the ENLYZE platform is associated with
exactly one :ref:`site <site>`.

.. _variable:

Variable
--------

A *variable* represents a process measure of one :ref:`appliance <appliance>` of which
timeseries data is captured and stored in the ENLYZE platform. One appliance may have
many variables, whereas one variable is only associated with one appliance.
