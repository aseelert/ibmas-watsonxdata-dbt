# Serve Gold in Metabase

Metabase is the business-consumption layer in this demo. It should point to
Gold marts, not to Raw or Silver tables: a dashboard becomes trustworthy when
its grain, metrics, and ownership are explicit upstream.

![Metabase dashboard home](../assets/images/screenshots/metabase-home.png)

## What the audience should see

1. The dashboard reads a Presto-accessible Gold relation.
2. A metric such as daily revenue has one declared transformation path.
3. A user can drill into the documented Gold data product without rebuilding
   joins in every chart.
4. The same Gold contract can be validated independently with SQL.

Metabase is intentionally interchangeable with Cognos, Power BI, watsonx BI,
or another compatible BI tool. The workshop is about the open table and the
business contract, not about making a particular dashboard product the data
platform.

Metabase is the primary business-facing UI in this workshop. It connects to the
same Presto catalog and provisions example queries over the Gold marts. Show it
after the dbt baseline to anchor the technical work in a business outcome.

```bash
bin/demo metabase
```

![Metabase home](../assets/images/screenshots/metabase-home.png)

The dashboard is a consumer of Gold. It does not define the metric logic, run
the transformation, or replace catalog governance. Those responsibilities stay
with the transformation contract and governance tooling.
