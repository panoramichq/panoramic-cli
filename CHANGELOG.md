# Changelog

## Version 1.7.1 Release

- Bugfix for badly generated transform queries

## Version 1.7.0 Release

- Introduces Pano Transforms
- A new `pano transform` command group
- A `pano transform create` command to help with transform file scaffolding
- A `pano transform exec` command to execute/compile transforms on top of a connection

## Version 1.6.0 Release

- Implement anonymous usage metrics collections

## Version 1.5.0 Release

- Introduce DBT subcommand to manage pre-model transforms
- Fix Homebrew install

## Version 1.5.0b1 Release

- Fix Homebrew install

## Version 1.5.0b0 Release

- Introduce DBT subcommand to manage pre-model transforms

## Version 1.4.0 Release

- Fix delete action for fields
- Introduce new configuration structure

## Version 1.3.3 Release

- Change `pano field scaffold` to scan field metadata
- Do not output field files during `pano scan`

## Version 1.3.2 Release

- Update README and global help for pano command

## Version 1.3.1 Release

- Fix scanned field files having model suffix
- Add `pano field cleanup` command
- Add `pano field scaffold` command

## Version 1.3.0 Release

- Deprecates `data_type` attribute in model files
- Validation of fields in model and field files
- Stop sending `create_fields=true` to Model API

## Version 1.2.0 Release

- Introduces field file management via the CLI

## Version 1.1.3 Release

- Send `create_fields` query param when upserting models

## Version 1.1.2 Release

- Fix message shown when pushing/pulling models and datasets

## Version 1.1.1 Release

- Add validation check for multiple models having the same model name.

## Version 1.1.0 Release

- This release introduces a new `detect-joins` CLI command.

## Version 1.0.2 Release

- Disallow `"one_to_many"` and `"many_to_many"` join types, since they are not supported anyway.

## Version 1.0.1 Release

- Sort model attributes alphabetically where order is not relevant. Mostly to prevent displaying diffs on those attributes.

## Version 1.0.0 Release

- Initial public release of the Panoramic CLI tool.
