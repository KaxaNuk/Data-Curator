---
name: debug-fmp
description: >
  Load this skill whenever you need to debug issues related to the data returned by FinancialModelingPrep endpoints.
metadata:
  version: 0.1
---

# Debug FinancialModelingPrep Endpoints

## Usage

1. **Identify the Problem Endpoints**: Determine which FinancialModelingPrep endpoints are causing the issue. Usually this
    involves tracing the code of the `FinancialModelingPrep` backwards from the code line that produced the error.
2. **Determine Endpoint Parameters**: The `FinancialModelingPrep` class encodes all endpoints we're using, and has
    methods that call those endpoints, from which you can see the expected parameters.
3. **API Key Handling**: Check that the API key is already set as the `KNDC_API_KEY_FMP` environment variable. If not, there
    should exist a `Config/.env` file in the root of the project, which should be exported into the environment to
    make all env vars available to the shell. **NEVER HANDLE THE API KEY DIRECTLY**, always use the `KNDC_API_KEY_FMP`
    env var with automatic shell interpolation.
4. **Request the Raw Data**: Use curl or similar to request the raw data returned by each relevant endpoint, with the 
    appropriate parameters for retrieving the data that exhibits the issue. If in doubt of which parameters to use, ask
    the user.
5. **Diagnose the Issue**: Determine which code expectations of the data returned by the endpoints were not met, and
    how we can determine at runtime that this exact issue with the data is happening.

Do not propose any silent fixes. All errors on the data side need to be clearly logged, and depending on the severity
of the issue, either the entity corresponding to the wrong data or the whole ticker's data will need to be skipped from
output by using the library's error handling system.
