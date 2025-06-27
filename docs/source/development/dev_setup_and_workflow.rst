.. _dev_setup_and_workflow:

Setting Up Your Development Environment
=======================================

1. **Clone and Fork**

   ::

       git clone https://github.com/<your-username>/data-curator.git
       cd data-curator
       git remote add upstream https://github.com/data-curator/data-curator.git
       git fetch upstream

2. **Install PDM and Development Dependencies**

   ::

       pip install pdm
       pdm install
       pdm run install_dev

3. **(Optional) Create a Virtual Environment**
   Although PDM manages environments automatically, if you prefer a venv or Conda, activate it before running ``pdm install``.

4. **Install Data Curator in Editable Mode**

   ::

       pdm run install_dev

5. **Verify the Test Suite**

   ::

       pdm run test

6. **Run the Linter**

   ::

       pdm run lint

Branching and Pull Request Workflow
-----------------------------------

Users without write access must fork the repository; those with write access may create branches directly.

Forking and Synchronizing
~~~~~~~~~~~~~~~~~~~~~~~~~

- Always keep your local ``main`` branch in sync:

  ::

      git checkout main
      git pull upstream main --ff-only

Creating a Feature Branch
~~~~~~~~~~~~~~~~~~~~~~~~~

- ::

      git checkout -b feature/<short-description>

  Example:

      git checkout -b feature/rolling-average-fix
