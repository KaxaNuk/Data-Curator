:html_theme.sidebar_secondary.remove:

Data Curator
============

Tool for **building a structured database for market, fundamental, and alternative data** from various financial data providers through their APIs.

Data Curator includes a set of **prebuilt calculation functions**, see the :ref:`features` section for the full list.

**To define your own logic**, you can write custom functions in Python using the DataColumn architecture; refer to :ref:`custom_calculations` to learn how.

Currently supported data providers:

- **Financial Modeling Prep** (see :ref:`fmp`)
- **Yahoo Finance** (via separate extension: `kaxanuk.data-curator-extensions.yahoo-finance <https://pypi.org/project/kaxanuk.data-curator-extensions.yahoo-finance/>`_, supports limited data types)

**Seamlessly integrate your own in-house datasets** with our flexible plugin architecture.

Interested in collaborating on a custom project? Get in touch at software@kaxanuk.mx

Check the `official release on PyPI <https://pypi.org/project/kaxanuk.data-curator/>`_ for the latest version.

.. grid:: 1 2 2 2
   :gutter: 4
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: User guide
      :class-card: intro-card
      :shadow: none

      Discover the fundamentals of Data Curator, learn how to set up your environment,
      and explore the essential features and workflows to get started quickly.

      +++

      .. button-ref:: user_guide
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         To user guide

   .. grid-item-card:: Data Providers
      :class-card: intro-card
      :shadow: none

      Understand how to integrate external and in-house data providers.
      You can also implement custom providers using our open architecture.

      +++

      .. button-ref:: data_providers
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         To data providers

   .. grid-item-card:: API Reference
      :class-card: intro-card
      :shadow: none

      Dive into the comprehensive API documentation, including Excel configuration,
      main modules, calculations, public objects and interfaces.

      +++

      .. button-ref:: api_reference
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         To API reference guide

   .. grid-item-card:: How to Contribute
      :class-card: intro-card
      :shadow: none

      Find detailed guides on setting up the project in editable mode, running tests, and contributing
      to the development of Data Curator. Ideal for developers looking to extend or enhance the tool.

      +++

      .. button-ref:: development
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         To development guide

.. _toc.columnar:

.. toctree::
   :maxdepth: 2
   :hidden:

   user_guide/index

.. _toc.userguide:

.. toctree::
   :maxdepth: 2
   :hidden:

   data_providers/index

.. _toc.api:

.. toctree::
   :maxdepth: 2
   :hidden:

   api_reference/index

.. _toc.development:

.. toctree::
   :maxdepth: 2
   :hidden:

   development/index

.. _toc.changelog:

.. toctree::
   :maxdepth: 1
   :hidden:

   release_notes/index
