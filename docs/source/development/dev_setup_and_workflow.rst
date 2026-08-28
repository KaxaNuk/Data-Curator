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

5. **Install the AI scaffolding for your AI coding assistant**
   You can check the available targets at https://github.com/microsoft/apm/blob/main/docs/src/content/docs/concepts/primitives-and-targets.md#target-catalogue
   The following example is for Claude:

   ::

       apm config set target claude
       apm install

6. **Verify the Test Suite**

   ::

       pdm run test

7. **Run the Linter**

   ::

       pdm run lint

Branching and Pull Request Workflow
-----------------------------------

Users without write access must fork the repository; those with write access may create branches directly, always based on the ``dev`` branch.

Forking and Synchronizing
~~~~~~~~~~~~~~~~~~~~~~~~~

- Always keep your local ``dev`` branch in sync:

  ::

      git checkout dev
      git pull upstream dev --ff-only

Creating an Issue Branch
~~~~~~~~~~~~~~~~~~~~~~~~~

When contributing, all contributions must be based on an existing registered issue on the Data Curator GitHub repository. When created, each issue gets assigned an issue number, and the contribution branch name should be
of the form ``issues/<issue-number>`` where ``<issue-number>`` is the number of the issue.

- ::

      git checkout -b issues/<issue-number>

  Example:

      git checkout -b issues/4
