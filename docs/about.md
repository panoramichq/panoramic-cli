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
    fields
    scanned
```

pano.yaml file must be at the root of the project and must contain:

```yaml
api_version: v1
```

Datasets represent logically related data models. Dataset and model have are semantic descriptions of database schemas and tables. Fields are description of columns in tables. Fields may simply point to existing columns, or they may contain calculations, using TEL functions, or by combining of other fields.

Fields on the project level contain global fields, shared by all models.

NOTE: Schemas for all descriptor files can be found under src/panoramic/schemas directory.

<a name="dataset.yaml"></a>
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

The `data_source` refers to the FQN of the table/view. `data_reference` is the model TEL expression, typically a name of a column. `field_map` refers to one or more taxons.

### Joins definitions
Joins are an array of join definitions. Join definition has following properties:
- `join_type`: either of `left`, `right` or `inner`
- `relationship`: `one_to_one` or `many_to_one`
- `to_model`: name of other model to join on
- `fields`: array of fields (field slugs) to use in the join

## Field YAML file
First terminology. Field and taxon are the refer to the same thing, just historically fields were called taxons.
Raw taxons are those which are directly pointing to an existing database column. Computed taxons contain use some computations performed outside of the existing columns.
Field slugs for fields defined under a dataset must be namespaced, by using this syntax: `model_slug|field_slug`. The pipe operator denotes a namespace.
Global fields must not have any namespace and must live under the `project root/fields` directory.

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

- `aggregation` a specification of the aggregation type, see section below
- `data_type` must be a valid data type, see table below
- `api_version` must be set to `v1`
- `display_name` is just a human readable name
- `field_type` is either `metric` or `dimension`
- `slug` is the ID of the field
- `group` is a logical group of the field, but there is no further functionality related to it. It may have any value at the moment
- `calculation` an optional TEL calculation expression, see further explanation in the section below

#### Data Types
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

#### Aggregation
An aggregation definition has two properties:
- `type`: either of `sum`, `min`, `max`, `avg`, `count_all`, `count_distinct`, `group_by`, `first_by`, `last_by`
- `params`: dependening on a type, it contains:
    - `fields` for `count_distinct`, containing field slugs
    - `sort_dimensions` for `first_by` and `last_by`, as an array of object with following properties:
        - `taxon`: field slug
        - `order_by`: `asc` or `desc`

## Transformation YAML file
Transformation files use the model definitions to build views that can be deployed to the database, using a connection stored in the `pano` tool configuration. To create a transformation, run:
```sh
pano transform create
```

This command will prompt the connection to use, and the target (the resulting view FQN). Then one must edit this transform file, stored under transformations subfolder, and add fields to include in this view. After that, transformations may be deployed by executing:

```sh
pano transform exec
```

Optionally, one might pass `--compile` argument to the `transform exec` command to only get the resulting SQL views created (they will be storred in transformations/.compiled/ folder.)

# Calculated fields
Calculated fields must not have an `aggregation` property and instead they must provide a `calculation` property. Value of this property is the formula in TEL language.
Simplest example could be a sum of two fields, `field1 + field2` for instance. Alternatively, any supported TEL functions may be used, see the section below.

NOTE: aggregation type is deduced from the calculation field itself, therefore it must not be explicitely specified by the field definition file.

## TEL functions
See [tel-functions.md](https://github.com/akovari/panoramic-cli/blob/docs/docs/about.md#tel-functions) document for details. There are two dialects of TEL language, one for calculations and one for `data_reference`s in field definitions, that dialect is called "model TEL". The main difference is that "model TEL" uses double quote strings to denote SQL columns, but there may be few minor other differences, as outlined in the linked document.

# Connections
Connections in `pano` use [SqlAlchemy](https://www.sqlalchemy.org/), so there must be an appropriate SqlAlchemy connector present in user's Python path, and then connection must be created using the appropriate connection string for this connector.

Managing connections with `pano` is done through the `connection` command:
```sh
pano connection -h
```

## Snowflake example
Creating a Snowflake connection:

```sh
pano connection create --type snowflake conn_name --url 'snowflake://<user_login_name>:<password>@<account_name>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'

```

## BigQuery example
Creating a BigQuery conneciton:

```sh
pano connection create --type bigquery conn_name --url 'bigquery://'
```

Before running this, make sure, the environment variable `GOOGLE_APPLICATION_CREDENTIALS` is pointing to the file with Google [credentials](https://cloud.google.com/docs/authentication/production).

NOTE: It is also possible to create a "dummy" connection, by not specifying the `--url`, but instead by providing a `--dialect <snowflake | bigquery>` option.

# Metadata scanning

You can let the tool to inspect your database storage and generate models & fields for them.

Run following command to scan your database storage for any models

```she
pano scan conn_name
```

Now, you have all scanned models in directory `<project root>/scanned`.
You should, either create a new dataset (create a new directory in project root with [dataset.yaml](#dataset.yaml) file),
or copy the relevant models from this directory into desired dataset.

At the moment, you may be missing definitions for some field files (used in newly created models).
Run following command to generate their definitions:

```sh
pano field scaffold
```

By default, it scans the DB again to determine data types for missing fields.
You may suppress this behavior by adding flag `--no-remote`. In that case, all fields are created as text dimensions.