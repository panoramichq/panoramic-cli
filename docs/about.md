# Panoramic CLI
This tool uses declarative database model descriptions and transformation declarations to create and deploy database views. Currently supported data warehouses are Snowflake and Google Big Query.

# Project structure
The model descriptions and transformations are stored as YAML files in the following structure:

```
<project root>
    pano.yaml
    <dataset directories>
        dataset.yaml
        <model files>.yaml
        fields
            <field files>.yaml
    transformations
        <transformation files>.yaml
```

pano.yaml file must be at the root of the project and must contain:

```yaml
api_version: v1
```

Datasets represent logically related data models. Dataset and model have are semantic descriptions of database schemas and tables. Fields are description of columns in tables. Fields may simply point to existing columns, or they may contain calculations, using TEL functions, or by combining of other fields.

NOTE: Schemas for all descriptor files can be found under src/panoramic/schemas directory.

## dataset.yaml
Dataset descriptor contains a slug (identifier) and a display name (this is just a human readable name).

```yaml
api_version: v1
dataset_slug: dblp
display_name: DBLP
```

## Model YAML file
There may be multiple model files in the same directory as dataset.yaml. They must have suffix `.model.yaml` and their prefix must be the model name, typically in the following format `<database.schema.table>`, or any fully qualified table name, respecting the database used. For example, full model file could be called `database.public.table.model.yaml`. This file would contain:

```yaml
api_version: v1
data_source: database.public.table
fields:
- data_reference: '"KEY"'
  field_map:
  - person_key
- data_reference: '"MONEY_SPENT"'
  field_map:
  - money_spent
identifiers:
  - person_key
joins: []
model_name: database.public.table
```

TBD joins.

## Field YAML file
Fields may be automatically scaffolded (pre-created) by running:
```sh
pano field scaffold
```

This will create YAML files for fields defined in all the model files. They look for example something like:
```yaml
aggregation:
  type: group_by
api_version: v1
data_type: text
display_name: person key
field_type: dimension
group: CLI
slug: person_key
```

### Description of the properties

- TBD aggregations
- `data_type` must be a valid data type, see table below.
- `api_version` must be set to `v1`
- `display_name` is just a human readable name
- `field_type` is either `metric` or `dimension`
- `slug` is the ID of the field
- `group` is a logical group of the field, but there is no further functionality related to it. It may have any value at the moment.

### Data Types
Supported data types are from the list below. They translate roughly to equivalent column types in supported database.
- text
- integer
- numeric
- datetime
- enum (aka predefined list)
- percent
- money
- url
- boolean
- duration
- variant

## Transformation YAML file
Transformation files use the model definitions to build views that can be deployed to the database, using a connection stored in the `pano` tool configuration. To create a transformation, run:
```sh
pano transform create
```

This command will propmpt the connection to use, and the target (the resulting view FQN). Then one must edit this transform file, stored under transformations subfolder, and add fields to include in this view. After that, transformations may be deployed by executing:

```sh
pano transform exec
```

# Calculated fields
Calculated fields must not have an `aggregation` property and instead they must provide a `calculation` property. Value of this property is the formula in TEL language.
Simplest example could be a sum of two fields, `field1 + field2` for instance. Alternatively, any supported TEL functions may be used, see the section below.

NOTE: aggregation type is deduced from the calculation field itself, therefore it must not be explicitely specified by the field definition file.

## TEL functions
See tel-functions.md document for details.

# Connections
Connections in `pano` use [SqlAlchemy](https://www.sqlalchemy.org/), so there must be an appropriate SqlAlchemy connector present in user's Python path, and then connection must be created using the appropriate connection string for this connector.

Managing connections with `pano` is done through the `connection` command:
```sh
pano connection -h
```
