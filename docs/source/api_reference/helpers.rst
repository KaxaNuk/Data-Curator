.. _helpers:

Helpers
=======

.. currentmodule:: kaxanuk.data_curator.features.helpers

Calculation Helpers
--------------------------




This section lists all the **underlying calculation helper functions** provided by Data Curator.
These functions act as the mathematical and statistical engine for the main feature indicators.

To use one of these helpers:

- Import and call them directly within your custom calculations or when developing new feature modules.
- Unlike main features, they are not typically called directly from the Excel configuration file.

All functions operate on ``DataColumn`` inputs and return new ``DataColumn`` objects,
ensuring compatibility with our internal data.

Available Helper Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 100

   * - Function
   * - :ref:`annualized_volatility <annualized_volatility_helper_ref>`
   * - :ref:`chaikin_money_flow <chaikin_money_flow_helper_ref>`
   * - :ref:`exponential_moving_average <exponential_moving_average_helper_ref>`
   * - :ref:`indexed_rolling_window_operation <indexed_rolling_window_operation_helper_ref>`
   * - :ref:`log_returns <log_returns_helper_ref>`
   * - :ref:`relative_strength_index <relative_strength_index_helper_ref>`
   * - :ref:`replace_infinite_with_none <replace_infinite_with_none_helper_ref>`
   * - :ref:`simple_moving_average <simple_moving_average_helper_ref>`

.. toctree::
   :hidden:
   :maxdepth: 1

   helpers_api/available_helper_functions/index
