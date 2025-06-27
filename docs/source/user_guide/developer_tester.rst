.. _developer_tester:

Developer/Tester Workflow
=========================

In “Developer/Tester” mode, you set up a development environment, run linters and tests, and contribute code to the Data Curator project. This guide assumes you have Python 3.12 installed and are familiar with CLI operations. Follow these steps to clone the repository, install development dependencies, run linters and tests, and prepare pull requests.

Dev Setup
---------

Install PDM
~~~~~~~~~~~

First, install PDM (Python Development Master) if it’s not already available:

.. code-block:: bash

   pip install pdm

Clone the Repository
~~~~~~~~~~~~~~~~~~~~

Next, download the Data Curator source code from GitHub. Replace `<THE_REPO_URL>` with the actual repository URL:

.. code-block:: bash

   git clone <THE_REPO_URL>
   cd <REPO_DIRECTORY>

Install in Editable Mode with Dev Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inside the repository directory (where `pyproject.toml` resides), install the project in editable mode along with its development dependencies:

.. code-block:: bash

   pdm run install_dev

At this point, your virtual environment (managed by PDM) contains all runtime and dev tools required to lint, test, and build documentation. :contentReference[oaicite:0]{index=0}

Development Tasks
-----------------

Example Scripts
~~~~~~~~~~~~~~~

You can find example entry scripts under the `examples/` subdirectory. To experiment:

.. code-block:: bash

   # Copy an example script to your working directory
   cp examples/example_run.py ~/my_workdir/
   cd ~/my_workdir/
   # Modify and run it as needed
   python example_run.py

Run the Linter
~~~~~~~~~~~~~~

Ensure code style and formatting standards by running:

.. code-block:: bash

   pdm run lint

This invokes the configured linter (e.g., Ruff, Mypy) across the codebase and flags any violations. :contentReference[oaicite:1]{index=1}

Run the Test Suite with Coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Execute all automated tests and generate a coverage report:

.. code-block:: bash

   pdm run test

The test suite leverages `pytest` and is configured to produce coverage statistics. Ensure that any new code you add is covered by unit tests to maintain 100% coverage on modified or added lines. :contentReference[oaicite:2]{index=2}

Contributing Pull Requests
---------------------------

Before submitting a pull request, confirm mandatory criteria are met:

- A registered issue exists explaining the problem or feature request.
- All linter checks pass (no style or formatting errors).
- The entire test suite passes without errors.
- Code coverage does not decrease; any new or modified code must include corresponding tests covering 100% of those changes.

Following these guidelines helps maintain code quality and ensures your contribution is accepted. :contentReference[oaicite:3]{index=3}

Next Steps
----------

- **Explore Example Tests**
  Examine existing test modules (e.g., under `tests/`) to understand patterns for writing new unit tests. :contentReference[oaicite:4]{index=4}
- **Integrate CI/CD**
  Configure GitHub Actions (or another CI system) to automatically run `pdm run lint` and `pdm run test` on pull requests.
- **Update Coverage Tools**
  If needed, adjust `pytest` and coverage settings (e.g., in `pyproject.toml`) to ensure coverage reports meet project standards.
- **Maintain Code Quality**
  Regularly run `pdm run lint` and `pdm run test` during development to catch issues early.

See also
--------

- :ref:`Zero-Coder Workflow <zero_coder>` for end-user installation and usage.
- :ref:`Custom Calculator Workflow <custom_calculator>` for adding Python-based features.
- :ref:`Component Integrator Workflow <component_integrator>` for programmatic integration.
