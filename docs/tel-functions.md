# Supported TEL Functions

### `coalesce`: return the first non-null value in a list

Supported dialects: model, taxon

`coalesce` function accepts any number of arguments and returns the first valid, non-null value

> Example

```
coalesce(?facebook_ads|spend, twitter|spend)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | any | (many) any expression

**Returns**:
(*any* type) the first valid, non-null, argument.

**Raises Validation Error**

When |
---- |
`(number of arguments) == 0` |
arguments are not of compatible types |
        

### `concat`: concatenation, or joining, of two or more string values in an end-to-end manner

Supported dialects: model, taxon

`concat` function returns a concatenation of its arguments, without any separator. Arguments must be from the same data source

> Example

```
concat(?twitter|ad_id, ?twitter|ad_name)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | any | (many) expression to concatenate on the output

**Returns**:
(*string* type) concatenation of the provided strings.

**Raises Validation Error**

When |
---- |
taxon fields are from different data sources or before `merge()` was applied |
        

### `contains`: check whether provided string expression contains any of provided search constants

Supported dialects: model, taxon

`contains` function accepts a string, dimension, taxon field and one or more string constants of which at least one needs to be contained in the string expression (Note: this operation is case-sensitive)

> Example

```
contains(campaign_id, 'bud79')
```

> Example for case-insensitive search, by combination with the `lower` function

```
contains(lower(twitter|ad_id), 'id')
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | string | string, dimension taxon to perform search on
`searched_constant` | string | (many) search constants to search in the specified `expression`

**Returns**:
(*boolean* type) `true` if the expression contains at least one of the specified `searched_constant`s, `false` otherwise.

**Raises Validation Error**

When |
---- |
`(number of arguments) < 2` |
argument is of invalid type |
`expression` is in an aggregation phase |
        

### `convert_timezone`: converts a timestamp to another timezone

Supported dialects: model, taxon

`convert_timezone` converts a timestamp expression from source to destination timezone. For timestamps with timezone
just provide destination timezone. For timestamps without timezone you have to provide both source and destination timezones.

> Example for timestamp with timezone

```
convert_timezone(timestamp_tz, "Europe/Prague")
```

> Example for timestamp without timezone

```
convert_timezone(timestamp_ntz, "America/Los_Angeles", "Europe/Prague")
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | taxon datetime field to be converted
`timezone_from` | string | (optional) source timezone to convert from, supported time zones are defined by IANA database
`timezone_to` | string | destination timezone to convert to, supported time zones are defined by IANA database

**Returns**:
(*datetime* type) datetime in the specified destination timezone.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 2 or 3` |
`expression` is in an aggregation phase |
`timezone_from` is not a valid IANA timezone name |
`timezone_to` is not a valid IANA timezone name |
        

### `cumulative`: cumulative window function

Supported dialects: taxon

`cumulative` calculates aggregated values using cumulative window frame. Values are divided into windows using all request dimensions excluding the time dimension,
which is used to order the values. Aggregation type is derived automatically.

> Example

```
cumulative(spend, date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`metric` | numeric | metric to apply cumulative aggregation to
`time_dimension` | datetime | time dimension to order the values by

**Returns**:
(*numeric* type) cumulated metric value for each row.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 2` |
argument is of invalid type |
`time_dimension` is not a dimension |
aggregation type couldn't be derived or is not supported |
        

### `date`: reduce granularity of a time taxon to daily granularity

Supported dialects: model, taxon

`date` function drops the time component of the specified datetime taxon field, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/22`

> Example
```
date(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field to apply the reduction to

**Returns**:
(*datetime* type) result of this datetime transformation function.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `date_diff`: calculate difference between two datetime values, in the specified time unit

Supported dialects: model, taxon

`date_diff` function accepts time unit, in which the difference between two date time values is returned. Start and end values must be from the same data source and they must not be in the aggregation phase or later

> Example
```
date_diff('MINUTE', twitter|date, merged_date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`time_unit` | string | one of supported time unit values (supported values are: `"SECOND"`, `"MINUTE"`, `"HOUR"`, `"DAY"`, `"WEEK"`, `"MONTH"`, `"YEAR"`
`start_time` | datetime | start time for calculation of time difference
`end_time` | datetime | end time for calculation of time difference

**Returns**:
(*integer* type) the difference between two dates, in the unit specified by the `time_unit` argument.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 3` |
argument is of invalid type |
`time_unit` is an unknown time unit value |
`start_time` or `end_time` is in an aggregation phase |
`start_time` and `end_time` are from different data sources |
        

### `date_hour`: reduce granularity of a time taxon to hourly granularity

Supported dialects: model, taxon

`date_hour` function drops minutes and seconds of the specified datetime taxon field, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/22 09:00:00`

> Example
```
date_hour(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field to apply the reduction to

**Returns**:
(*datetime* type) result of this datetime transformation function.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `date_month`: reduce granularity of a time taxon to monthly granularity

Supported dialects: model, taxon

`date_month` function drops the time component of the specified datetime taxon field, and returns the date of the beginning of the month date, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/01`

> Example
```
date_month(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field to apply the reduction to

**Returns**:
(*datetime* type) result of this datetime transformation function.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `date_trunc`: return the time portion of the date time truncated to the unit

Supported dialects: model

`date_trunc` function reduces the

> Example
```
date_trunc(twitter|date, 'HOUR')
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field to apply the reduction to
`string` | unit | string literal, one of: HOUR, DAY, WEEK or MONTH

**Returns**:
(*datetime* type) result of this datetime transformation function.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 2` |
argument is of invalid type |
        

### `date_week`: reduce granularity of a time taxon to weekly granularity

Supported dialects: model, taxon

`date_week` function drops the time component of the specified datetime taxon field, and returns the date of the beginning of the week date, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/17`

> Example
```
date_week(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field to apply the reduction to

**Returns**:
(*datetime* type) result of this datetime transformation function.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `day_of_week`: return the day of week associated with the datetime taxon field

Supported dialects: model, taxon

`day_of_week` function returns the day of week of the provided datetime taxon field

> Example
```
day_of_week(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field

**Returns**:
(*integer* type) the day of the week associated with the datetime value.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `hour_of_day`: return the hour of day associated with the datetime taxon field

Supported dialects: model, taxon

`hour_of_day` function returns the hour of the day of the provided datetime taxon field

> Example
```
hour_of_day(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field

**Returns**:
(*integer* type) the hour of the day associated with the datetime value.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `iff`: if-then-else condition

Supported dialects: model, taxon

`iff` is a function of two or three arguments, depending on whether there is an `else` part or not

> Example with only true outcome

```
iff(twitter|spend > 100, impressions + twitter|spend - 2)
```

> Example with both outcomes

```
iff(twitter|spend > 100, impressions / 100, impressions * 2)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`condition` | boolean | conditional expression, in the same phase as the outcome arguments (dimension or metric)
`positive_outcome` | any | any expression in the same phase as the condition, returned when the condition evaluated to true
`negative_outcome` | any | (optional) any expression in the same phase as the condition, returned when the condition evaluated to false

**Returns**:
(*any* type) either result of the `positive_outcome` or `negative_outcome` expression, depending on the result of the `condition`.

**Raises Validation Error**

When |
---- |
(number of arguments) != 2 or 3 |
positive and negative outcome have different result phases |
the outcomes have incompatible return types |
        

### `ifs`: switch expression

Supported dialects: model, taxon

`ifs` is a function of up to 100 pairs of a condition and an outcome expression with one additional, optional `else`
expresion that is returned, if none of the conditions matched

> Example with two conditions and no `else` expression

```
ifs(fb_tw_merged_ad_id == 'tw', twitter|ad_id, fb_tw_merged_ad_id == 'fb', facebook_ads|ad_id)
```

> Example with two conditions and an `else` expression

```
ifs(fb_tw_merged_ad_id == 'tw', twitter|ad_id, fb_tw_merged_ad_id == 'fb', facebook_ads|ad_id, "unknown")
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`condition` | boolean | (many) conditional expressions, in the same phase as the outcome arguments (dimension or metric)
`positive_outcome` | any | (many) any expressions in the same phase as the condition, returned when the condition evaluated to true
`negative_outcom` | any | (optional) any expression in the same phase as the condition, returned when all the conditions evaluated to false

**Returns**:
(*any* type) the first result of the `positive_outcome` of the pair where the `condition` matched, or else the `negative_outcome` expression, if specified.

**Raises Validation Error**

When |
---- |
there were less than 2 arguments or more than 100 pairs of condition and outcome |
there are outcomes in both dimension and metric outcomes |
the outcomes have incompatible return types |
        

### `lower`: convert string to lower-case

Supported dialects: model, taxon

`lower` function converts provided string taxon field to lower-case string

> Example

```
lower(fb_tw_merged_objective)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`taxon` | string | string, dimension, taxon field to be converted to lower-case

**Returns**:
(*string* type) lower-case converted provided taxon.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
`expression` is in an aggregation phase |
        

### `merge`: return the first valid, non-null, value across multiple data sources

Supported dialects: taxon

`merge` is the only function that can combine dimensions across data sources. It allows only one taxon per data source.

> Example:

```
merge(?twitter|ad_id, ?facebook_ads|ad_id)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | any | (many) expression, potentially, from different data-sources, from which the function will pick the first non-null, valid value as the result

**Returns**:
(*any* type) the first valid, non-null, value across multiple data sources.

**Raises Validation Error**

When |
---- |
`(number of arguments) < 1` |
arguments are not of compatible types |
there are more than one taxon fields per data source |
        

### `month_of_year`: return the month of the year associated with the datetime taxon field

Supported dialects: model, taxon

`month_of_year` function returns the month of year of the provided datetime taxon field

> Example
```
month_of_year(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field

**Returns**:
(*integer* type) the month of the year associated with the datetime value.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `now`: current date and time

Supported dialects: model, taxon

`now` returns current date and time

> Example

```
now()
```

**Arguments**

Name | Type | Description
---- | ---- | ------------


**Returns**:
(*datetime* type) current date and time.

**Raises Validation Error**

When |
---- |

        

### `overall`: overall window function

Supported dialects: taxon

`overall` calculates aggregated values for each row from the entire result. Aggregation type is derived automatically.

> Example

```
overall(spend)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`metric` | numeric | metric to apply aggregation to

**Returns**:
(*numeric* type) aggregated metric value for each row.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
aggregation type couldn't be derived or is not supported |
        

### `override`: overrides original value

Supported dialects: taxon

`override` changes values in TEL expression from the first argument to new values using mapping specified in the second argument

> Example (excluding missing values)

```
override(gender, 'our-gender-mapping', false)
```

> Example (including missing values)

```
override(gender, 'our-gender-mapping')
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`original_dimension` | string | Original dimension that we want to override
`override_mapping_slug` | string | Unique identification of the override mapping
`include_missing_values` | string | Controls whether values not present in the mapping should be part of the output under 'Unknown' value (default value is True)

**Returns**:
(*string* type) value of the override mapping.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 2 or 3` |
argument is of invalid type |
`original_dimension` not a dimension |
        

### `parse`: split the string by a delimiter and return n-th extracted value

Supported dialects: model, taxon

`parse` function takes a string expression, a delimiter and a position of the split result, which is then returned

> Example

```
parse(fb_tw_merged_objective, "|", 2)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | string | string, dimension, taxon field, which will be split by the `delimiter`
`delimiter` | string | string expression, used to separate the expression into parts
`position` | integer | 1-based index of the part to return

**Returns**:
(*string* type) requested part.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 3` |
argument is of invalid type |
`expression` is in an aggregation phase |
        

### `to_bool`: cast the value to a boolean data type

Supported dialects: model, taxon

`to_bool` function accepts number, string or boolean expression that is cast to boolean type. If the argument is a number, then 0 is converted to `false`, all other numbers are `true`.
If the argument is a string equal to `false` (case-insensitive), then it's a `false`, otherwise the result is `true`

> Example

```
to_bool(facebook_ads|done)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | number_string_boolean | an expression to be cast to the required type

**Returns**:
(*boolean* type) boolean representation of the value in the `expression` field.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `to_date`: cast the value to a date data type

Supported dialects: model, taxon

`to_date` function accepts number, string or date or datetime expression that is cast to date type,
using a format string to parse the input expression, unless it's a number expression

> Example

```
to_date(ad_name, 'YYYY-MM-DD')
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | number_string_date_datetime | an expression to be cast to the required type
`format` | string | (optional) format of the expression if it's a string

**Returns**:
(*date* type) date representation of the value in the `expression` field.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1 or 2` |
argument is of invalid type |
        

### `to_number`: cast the value to a number data type

Supported dialects: model, taxon

`to_number` function accepts any expression that is cast to a number type, integer or float, depending whether precision is specified or not

> Example

```
to_number(facebook_ads|done)
```

> Example, with a precision specified

```
to_number(facebook_ads|done, 2)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | number_string_boolean | an expression to be cast to the required type
`precision` | integer | (optional) when precision is specified, the result is cut off at this precision level (default value is 0)

**Returns**:
(*number* type) integer or float representation of the value in the `expression` field.

**Raises Validation Error**

When |
---- |
`(number of arguments) > 2` |
argument is of invalid type |
        

### `to_text`: cast the value to a string data type

Supported dialects: model, taxon

`to_text` function accepts any expression that is cast to string type

> Example

```
to_text(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | any | any expression to be cast to the required type

**Returns**:
(*string* type) string representation of the value in the `expression` field.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `trim`: strip leading and trailing whitespace (spaces, tabs and newlines)

Supported dialects: model, taxon

`trim` function strips leading and trailing whitespace (spaces, tabs and newlines)

> Example

```
trim(fb_tw_merged_objective)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`taxon` | string | string, dimension, taxon field to be converted to lower-case

**Returns**:
(*string* type) trimmed value of the provided taxon.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
`expression` is in an aggregation phase |
        

### `upper`: convert string to upper-case

Supported dialects: model, taxon

`upper` function converts provided string taxon field to upper-case string

> Example

```
upper(fb_tw_merged_objective)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`taxon` | string | string, dimension, taxon field to be converted to upper-case

**Returns**:
(*string* type) upper-case converted provided taxon.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
`expression` is in an aggregation phase |
        

### `week_of_year`: return the week of the year associated with the datetime taxon field

Supported dialects: model, taxon

`day_of_week` function returns the week of year of the provided datetime taxon field

> Example
```
week_of_year(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field

**Returns**:
(*integer* type) the week of the year associated with the datetime value.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        

### `year`: return the year associated with the datetime taxon field

Supported dialects: model, taxon

`year` function returns the year of the provided datetime taxon field

> Example
```
year(twitter|date)
```

**Arguments**

Name | Type | Description
---- | ---- | ------------
`expression` | datetime | dimension taxon field

**Returns**:
(*integer* type) the year associated with the datetime value.

**Raises Validation Error**

When |
---- |
`(number of arguments) != 1` |
argument is of invalid type |
        
