# Conda Recipe

`recipe.yaml` is used to package up Nemosis to publish on Conda forge.

More info on this file structure is available [here](https://conda-forge.org/docs/maintainer/example_recipes/pure-python/).

When the dependencies of Nemosis change in `../pyproject.toml`, we don't _need_ to update this file too.
Conda has scripts to check new releases on Pypi and adjust them accordingly.
