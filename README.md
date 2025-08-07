# Kingfisher Wells: A Databricks Asset Bundle Experiment

## Do the Lat/Lons from different vendors agree with each other?... **_Mostly!_**

The surface location for wells present from all three sources (
[S&amp;P Global](https://www.spglobal.com/commodity-insights/en/products-solutions/upstream-midstream-oil-gas),
[Enverus](https://www.enverus.com/products/enverus-core/),
and [Oklahoma Corporation Commission](https://gisdata-occokc.opendata.arcgis.com/)
) as of July 2025 are mapped using a simple [streamlit](https://streamlit.io/) app, deployed as a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles).

Most of this exercise was to learn and document my adventures with Databricks Asset Bundles and CI/CD workflows. Things are moving fast at Databricks, and there's a lot of out-of-date advice out there. The processes here worked as of August 2025.

### Spoiler: ~3% of Lat/Lon points are "misplaced" by over 500 meters relative to each other. That's about 250 wells in a single county.

## Medallion Architecture

We follow [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) to collect and process the data.

1. **BRONZE**

Start by downloading data as .csv from Enverus and OCC via their web APIs, writing files directly to an S3 "External Location" managed by Unity Catalog. I also uploaded a .csv from S&P since I don't have API credentials. In all three cases, I filtered simply by `county = 'KINGFISHER'`. Each .csv was loaded into its own Delta table in the `geodata.bronze` schema.

2. **SILVER**

I selected identity columns (UWI, County, Well Name) and surface Latitude/Longitude from each source table. S&P uses the NAD27 datum, so I transformed Lat/Lon fields to WGS84 to match other sources. I did not try to normalize/clean Lat/Lon, since I want to examine this spatially, as-is. These standardized tables were written to the `geodata.silver` schema.

3. **GOLD**

I composed a single dataframe from a union of `silver` tables and pivoted on the standardized ID to make a "wide" format to compare distance (via `ST_DISTANCESPHERIOD`) between all pairs of POINTs in a single row. I only want to compare "triplets" so I filtered out NULL geometries. The final table: `geodata.gold.well_surface_locations` contains about 8300 wells.

Once we get the final `well_surface_locations` table populated, we can export the data in parquet format and work with it offline. See `scripts/export_well_surfaces.py`.

## Streamlit Map

This wasn't a data cleaning exercise, but it is easy to find a few wells where the County or Longitude quadrant sign is wrong. At lower thresholds
we can see many expected cases where the WGS84 position is farther north and east than the NAD27 point. However, a significant number exhibit variance beyond a datum shift, suggesting real quality issues.

The default threshold (500m) limits the display to only wells where the location of any vendor disagrees with that of either of the other vendors by more than 500 meters.

![Kingfisher Map](images/map.png "Kingfisher Map")

## Process

First, let's define exactly what we are building here:

1. Jupyter notebooks demonstrating Medallion workflows
2. A Streamlit map to visualize results
3. Mechanism to deploy both `dev` and `prod` environments
4. Github actions for tests and deployments

...and all of this needs to be deployable to Databricks in a IaC sort of way.

#### 0. Prerequisites

- You need a "real" Databricks account. The free sandbox will not suffice.
- Install and authenticate the databricks [CLI](https://docs.databricks.com/aws/en/dev-tools/cli/)
- Define a cluster that supports basic spatial (or try Mosaic or Sedona, etc.). I found lots of out-of-date information on this topic. Just create a cluster and try something like `SELECT st_astext(st_point(-71.104, 42.315, 4326))` to see if it works.
- Clone or compose your own repo to follow along. We'll need to edit some .yml files.

#### 1. Look up your DATABRICKS_HOST

The easiest way is to check the URL when you log in. It's the hash-like URL ending with `.cloud.databricks.com/` --something like this:

```
https://dbc-4567abcd-1234.cloud.databricks.com/
```

#### 2. Look up your cluster ID

The simplest way is to use the CLI:

```
databricks clusters list

ID Name State
1234-364325-jc3o20pa cluster_17.1_beta RUNNING
```

We need the first column of the output. Ignore TERMINATED serverless ids.

#### 3. Store secrets used by API calls

For example, I stored my Enverus key in a secrets scope named `kingfisher_secrets`:

```
databricks secrets create-scope kingfisher_secrets
databricks secrets put-secret kingfisher_secrets enverus_secret_key
```

#### 4. Update DAB Config

There are more ways to configure and deploy Databricks Asset Bundles (DAB) than are shown here, YMMV. The databricks.yml defines the bundle's global metadata and deployment targets, while the app.yml defines specific app resources.

These are where we add the DATABRICKS_HOST and DATABRICKS_CLUSTER_ID.

_NOTE: The bundle name and app name are NOT the same, and the app name has a `dev` or `prod` suffix. Sometimes Databricks may assume paths that are not valid based solely on the names._

```
# databricks.yml

bundle:
  name: kingfisher-wells-streamlit

resources:
  apps:
    kingfisher_streamlit_app:
      name: kingfisher-streamlit-app-${bundle.target}
      source_code_path: src/app
      description: "Kingfisher County surface well comparison app (Streamlit ${bundle.target})"

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dbc-4567abcd-1234.cloud.databricks.com/
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

  prod:
    mode: production
    workspace:
      host: https://dbc-4567abcd-1234.cloud.databricks.com/
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
```

```
# app.yml


command:
  - streamlit
  - run
  - app.py
env:
  - name: DATABRICKS_HOST
    value: https://dbc-4567abcd-1234.cloud.databricks.com/
  - name: DATABRICKS_CLUSTER_ID
    value: 1234-364325-jc3o20pa

```

#### 5. Deploy Bundle

```
databricks bundle deploy -t prod
```

This copies most of the repo to your Workspace like this:
`/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}`. If all goes well, this should only take a few seconds.

This does _not_ deploy your app--it deploys the bundle from which the app can be deployed.

#### 6. Deploy App

```
databricks apps deploy kingfisher-streamlit-app-dev --source-code-path \
/Workspace/Users/bryan@purr.io/.bundle/kingfisher-wells-streamlit/dev/files/src/app
```

On the Databricks site, the App page may suggest an `apps deploy` string that is wrong for our dev/prod app. Watch out for two things:

- app name will have a `dev/prod` suffix
- `source_code_path` uses the bundle, not app name, a subfolder for `dev` or `prod`, and includes `src/app` for our streamlit app.

#### 7. Look up Service Principal

The act of deploying your bundle and app results in the creation of a Service Principal account. This account needs to access various resoures to run properly.

```
databricks service-principals list
72285317889453 800a62af-8761-4b80-a469-8c203c419123 app-1sesac kingfisher-streamlit-app-dev ACTIVE
```

We mostly use the guid-like "app id" string, not the actual name:

`800a62af-8761-4b80-a469-8c203c419123`

#### 8. GRANT "Data Reader" to Service Principal

Our app needs to read a Delta table: `geodata.gold.well_surface_locations`, so we need to grant permission to the Service Principal. In a notebook, you could use SQL for the CATALOG and SCHEMA:

```
GRANT USE CATALOG ON CATALOG geodata TO `<principal>`;
-- etc. for SCHEMA
```

However, as of Databricks CLI v0.260.0 (2025), there is no direct CLI subcommand to grant catalog privileges (such as databricks grants update catalog ...) that extends beyond what you can do with the SQL language.

Instead, you can automate privilege grants by running SQL from a Databricks notebook or via the REST API (Jobs API to run notebooks/scripts)—but not directly from a YAML resource file or a simple one-liner CLI flag.

Meh. 🙃 I didn't want to do that, although I would opt to define a Notebook job for it.

Conveniently, there is a built-in role in the Catalog UI named "Data Reader" that provides what we need (This is where you can use the Service Principal name).

![Data Reader](images/data_reader.png "Data Reader")

#### 8. Let the Service Principal use the Cluster.

The Service Principal needs to invoke Spark commands on the cluster. This CLI command uses the CLUSTER_ID and the Service Principal's app id to give those permissions.

```
databricks clusters update-permissions 1234-364325-jc3o20pa --json '{
  "access_control_list": [
    {
      "service_principal_name": "800a62af-8761-4b80-a469-8c203c419123",
      "permission_level": "CAN_ATTACH_TO"
    },
    {
      "service_principal_name": "800a62af-8761-4b80-a469-8c203c419123",
      "permission_level": "CAN_RESTART"
    }
  ]
}'
```

You can verify permissions on the cluster too:

```
databricks permissions get clusters 1234-364325-jc3o20pa
```

**At this point the DAB Streamlit app should be operational and reading from the `geodata.gold.well_surface_locations` table!**

## Github CI/CD

#### 9. Define Github Tests

NOTE: Github will look for a `requirements.txt` file in your repo root. I didn't want to install all the databricks libs used by the Streamlit DAB app, so I defined a minimal `requirements_ci.txt` file just for pytest.

[pytest.yml](.github/workflows/pytest.yml)

#### 10. Define Github Deploy

We deploy the bundle and app, and then run a pre-defined job that just runs the bronze-silver-gold notebooks in a sequence to ensure that we are working with the latest data.

See this excellent [post](https://apps-cookbook.dev/blog/automate-apps-deployments-dabs/) for how to deploy on a "Release". For now, I'll just leave it manual.

[deploy.yml](.github/workflows/deploy.yml)

#### 11. Repository Secrets

All those variables in the Github actions are defined as Repository Secrets. Here's how they are defined.

![Repo Secrets](images/repo_secrets.png)

DATABRICKS_HOST:

`https://dbc-4567abcd-1234.cloud.databricks.com/`

DATABRICKS_TOKEN:

```
databricks tokens create --lifetime-seconds 63113904 --comment "kingfisher-deploy"
{
  "token_info": {
    "comment":"kingfisher-deploy",
    "creation_time":1754529154191,
    "expiry_time":1817601154191,
    "token_id":"2ebf34823b33311246e25701afd60265897ec8caa27c74878384618ba63276f9"
  },
  "token_value":"dapib6b488f25856b0dc5adcb696d9041894"
}
```

(Paste the token_value string as the secret)

DATABRICKS_USER:

`your_email@whatever.com`

(the email used by your Databricks account)

DEPLOY_TARGET:

`prod` or `dev`

(not secret but it's convenient to keep them here) for a Release-based deployment to `prod` just in case.

## DESTROY!

This will nuke just about everything.

```
databricks bundle destroy -t dev --auto-approve
```

## Parting Thoughts

- I briefly experimented with Mosaic and Sedona. Sedona required non-trivial init scripts for the compute cluster; Mosaic was supported only for surprisingly older versions of Spark and Photon (13.x). I instead opted for DBR 17.1-beta, which natively supported spatial.

- Using 17.1-beta exceeded the supported versions of databricks-connect, which meant I could not take advantage of vscode integration. 😑

- Performance on the compute cluster was surprisingly bad. We are dealing with fewer than 30K rows with the simplest of all POINT geometries. The ideal scenario (duckdb + local parquet) is quite snappy. I'll investigate performance improvements later.

- Normal workflows would leverage the lower costs of a serverless SQL Warehouse to access the Delta table. However, SQL Warehouse (neither pro nor legacy) did not spatial functions, so I had to attach the bundled app to my 17.1-beta cluster. Ouch. Perhaps we can configure the instance properties elsewhere?
