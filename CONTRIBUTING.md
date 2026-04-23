# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

## Types of Contributions

### Report Bugs

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug, preferably with a simple code example that reproduces the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

You can never have enough documentation! Please feel free to contribute to any
part of the documentation, such as the official docs, docstrings, or even
on the web in blog posts, articles, and such.

### Submit Feedback

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Get Started!

Ready to contribute? Here's how to set up `ispypsa` for local development.

1. Download a copy of `nemosis` locally.
2. Install [`uv`](https://github.com/astral-sh/uv).
3. Install `nemosis` using `uv` by running `uv sync` in the project directory.
4. Use `git` (or similar) to create a branch for local development and make your changes:

    ```console
    $ git checkout -b name-of-your-bugfix-or-feature
    ```

6. When you're done making changes, check that your changes pass the tests:

    ```console
    $ uv run pytest tests/ \
        --ignore=tests/test_data_fetch_methods.py \
        --ignore=tests/test_errors_and_warnings.py \
        --ignore=tests/test_format_options.py \
        --ignore=tests/test_performance_stats.py \
        --ignore=tests/test_processing_info_maps.py
    ```

    The suite is fully offline and runs in under a minute. The five ignored files are legacy
    network-hitting tests slated for removal. CI runs the same invocation on every PR.

    For details on how the test suite is structured, how to add tests for new tables, and how
    fixtures are maintained, see [testing_and_maintenance.md](testing_and_maintenance.md).

7. Commit your changes and open a pull request.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include additional tests if appropriate.
2. If the pull request adds functionality, the docstrings/README should be updated.
3. The pull request should work for all currently supported operating systems and versions of Python.

## Code of Conduct

Please note that the `nemosis` project is released with a
[Code of Conduct](CONDUCT.md). By contributing to this project you agree to abide by its terms.
