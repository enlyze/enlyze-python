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


.. _production_order:

Production Order
----------------

A *production order* represents the goal of producing a certain quantity of a
given :ref:`product <product>`. This is how operators are told how much of which
product to produce. Production orders are usually created in an external ERP
system or MES and then synchronized into the ENLYZE platform. They are
referenced by an identifier which oftentimes is a short combination of numbers
and/or characters, like FA23000123 or 332554.

In the ENLYZE platform, a production order always encompasses the production of
one single :ref:`product <product>` on one single :ref:`appliance <appliance>`.


.. _production_run:

Production Run
--------------

A *production run* represents the real allocation of a :ref:`production order
<production_order>` on an :ref:`appliance <appliance>`. Usually, the operator of
the appliance uses some kind of interface to log the time when a certain
production order has been worked on. A production order oftentimes is not
allocated continously on an appliance due to interruptions like a breakdown or
weekend. Each of these individual, continous allocations is called a *production
run* in the ENLYZE platform. It always has a beginning and, if it's not still
running, it also has an end. The ENLYZE platform calculates different aggregates
from the timeseries data of the appliance for each production run.


.. _product:

Product
-------

A *product* is the output of the production process which is executed by an
:ref:`appliance <appliance>`, driven by a :ref:`production order
<production_order>`. In the real world, an appliance might have some additonal
outputs, but only the main output (the product) modelled in the ENLYZE platform.
Similarly to the production order, a product is referenced by an identifier
originating from a customer system, which is synchronized into the ENLYZE
platform.

During the integration into the ENLYZE platform, the product identifier is
chosen in such a way that :ref:`production runs <production_run>` of the same
product are comparable with one another. This is the foundation for constantly
improving the production of recurring products over time.
