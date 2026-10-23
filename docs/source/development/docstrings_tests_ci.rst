.. _docstrings_tests_ci:

Docstrings, Tests, and Continuous Integration
=============================================

Implementing Changes and NumPy-Style Docstrings
-----------------------------------------------

- **Code Style**
  - Conform to the ``ruff`` lint rules (``pdm run lint``).

- **NumPy-Style Docstrings**

  ::

      def c_example_function(input_column):
          """
          One-line summary.

          Extended description.

          Parameters
          ----------
          input_column : DataColumn
              Description.

          Returns
          -------
          DataColumn
              Description.
          """
          pass

Adding Tests
------------

- All calculation logic must include tests.
- Follow the :ref:`testing_guide` patterns.

Committing and Pushing
----------------------

- **Commit Messages**

  - ``feat(calc): add rolling_average function``
  - ``fix(calc): correct behavior on missing values``

- Every pull request needs to be based on a GitHub issue, so name your branch after its issue number:

  ::

      git push origin issues/<issue-number>

Opening a Pull Request
----------------------

- Summarize the change.
- Link the issue (e.g., “Closes #123”).
- Checklist before review:
  - [ ] Linked issue.
  - [ ] All linters/tests pass.
  - [ ] New code is tested.
  - [ ] Docstrings included.

Continuous Integration
----------------------

- CI runs on pushes and pull requests to ``main`` and ``dev``, on Python 3.12, 3.13 and 3.14:

  1. ``ruff check``
  2. ``pytest``

- For a change to be accepted:
  - All linters must pass.
  - All tests must pass.
  - New logic must be covered by tests.
