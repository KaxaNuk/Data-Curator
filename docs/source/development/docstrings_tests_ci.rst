.. _docstrings_tests_ci:

Docstrings, Tests, and Continuous Integration
=============================================

Implementing Changes and NumPy-Style Docstrings
-----------------------------------------------

- **Code Style**
  - Conform to lint rules (``ruff``, ``flake8``).
  - Format code with ``black``.

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
- Follow Testing Guide patterns.

Committing and Pushing
----------------------

- **Commit Messages**

  - ``feat(calc): add rolling_average function``
  - ``fix(calc): correct behavior on missing values``

- ::

      git push origin feature/rolling-average-fix

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

- CI includes:

  1. ``pdm run lint``
  2. ``pdm run test`` with coverage

- For a change to be accepted:
  - All linters must pass.
  - All tests must pass.
  - New logic must be covered by tests.
