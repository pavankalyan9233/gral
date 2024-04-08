# Setting up Graph Analytics Engines (GAEs) on ArangoGraph

It is now possible to deploy GAEs (graph analytics engines) on ArangoGraph.
Note that currently, this is behind a feature flag and has to be activated
by an employee of ArangoDB for your ArangoGraph organization. Note that every
GAE is attached to one particular ArangoDB database deployment on ArangoGraph.

Once this is done, you can follow the instructions below.

## Get API access to ArangoGraph

First you get yourself an API key for the ArangoGraph API under 
[this link](https://dashboard.arangodb.cloud/dashboard/user/api-keys).
There, you get an API key ID and an API key secret. We assume in the
following that you have stored these in two environment variables like so:

```bash
export AG_API_KEY_ID="gxhq0zjxpgrosoldeapc"
export AG_API_KEY_SECRET="1c5fe99b-4597-fa84-871d-5e9cb5915e45"
```

(values are faked). Then you can use the `oasisctl` command (download
an executable from [here](https://github.com/arangodb-managed/oasisctl/releases)) as follows to create an access token:

```bash
oasisctl login --key-id $AG_API_KEY_ID --key-secret $AG_API_KEY_SECRET
```

it will look something like this:

```
hvs.CAESIO4mXSkzOXV1zO_s-RTvc9BMg0GnJfM6NxfTo8U0xURHGh4KHGh2cy5zQ3g4dxp5eXJkakFLOHpOdzxFUEtUMGY
```

(this particular one is illegal).

You can use the following HTTP API calls by setting the `Authorization` header
to `bearer` followed by the token from above like so:

```
Authorization: bearer hvs.CAESIO4mXSkzOXV1zO_s-RTvc9BMg0GnJfM6NxfTo8U0xURHGh4KHGh2cy5zQ3g4dxp5eXJkakFLOHpOdzxFUEtUMGY
```

## Accessing the ArangoGraph API to deploy and undeploy GAEs

If your database deployment is available under the endpoint:

```
https://62535a263232.arangodb.cloud:8529
```

(say), then you need to access the ArangoGraph API under the URL

```
https://62535a263232.arangodb.cloud:8829/graph-analytics/api/graphanalytics/v1
```

(note the changed port 8829!). Let's call this `BASE_URL` for now.

## Available API calls:

The following API calls are available, all start with the URL above:

 - `GET $BASE_URL/api-version`: return a JSON document describing the API version
 - `GET $BASE_URL/enginetypes`: return a JSON document describing the available
   GAE types, currently, there is only one called `gral` available
 - `GET $BASE_URL/enginesizes`: return a JSON document describing the available
   GAE sizes, currently, there are a certain number of choices available,
   which basically choose the number of cores and the size of the RAM
 - `GET $BASE_URL/engines`: return a list of currently deployed GAEs for this
   database deployment
 - `GET $BASE_URL/engine/<id>`: return information about a specific GAE
 - `POST $BASE_URL/engines` with a body like this:

```json
{"type_id":"gral", "size_id": "e32"}'
```

   This will deploy an engine of type `gral` with size `e32`, which means
   32 GB of RAM and 8 cores.

 - `DELETE $BASE_URL/engine/<id>`: undeploy (delete) a specific GAE

All these API calls can be executed conveniently with the provided `gae`
shell script. Just set the environment variables `ARANGO_GRAPH_TOKEN` to
the access token described above and `DEPLOYMENT_URL` to the URL of your
deployment (leaving out the port part `:8529`).

The `gae` tool is used as follows:

```
gae <subcommand>

  where <subcommand> is any of
    - version           : show version
    - enginetypes       : show possible engine types
    - enginesizes       : show possible engine sizes
    - engines           : show currently deployed engines
    - engine <ID>       : show info about engine with ID <ID>
    - deploy            : deploy an engine, needs a body with '-d' arguments
          as for curl, example:
          -d '{"type_id":"sloth-main", "size_id": "e4"}'
    - delete <ID>       : delete the engine with ID <ID>
    - api <API> <BODY>  : call engine, needs ENGINE_URL env variable set
```

For this to work, you need `bash` and `curl` installed.

## Using the API of a GAE

If you use the `GET $BASE_URL/engine/<id>` API (or the `gae engines` call),
you get for each deployed GAE under `status` an `endpoint` entry, which
has a URL endpoint like this one:

```
https://62535a26323.arangodb.cloud:8829/graph-analytics/engines/qgeqbcxmktk0adqxd9pn
```

This endpoint URL starts like your deployment URL, uses port 8829 and then
has the ID of your particular GAE deployment in the path.

Let's call this `ENGINE_URL` for now.

You then have API calls on the engine like

```
GET $ENGINE_URL/v1/version
```

to get the version of the particular GAE running. The API of the GAE is
documented below. The documentation is automatically generated from the
protobuf API description, so that the bodies of the HTTP requests are
documented in detail.

A critical point is the authentication for these API calls.

There are two modes, in which your database deployment can be:

 1. Platform authentication switched on.
 2. Platform authentication switched off.

The authentication for API calls to the GAE works differently for these two
cases.

In Case 1. you have to give the exact same authorization header as
above with an ArangoGraph access token. The platform will automatically
verify the validity of the token and, if authenticated, will change
the authorization header on the fly to provide one with a JWT token,
which the GAE can use to access the database deployment! This means
in particular, that the GAE will have the access permissions of the
ArangoGraph platform user, which also exists as an ArangoDB user in the
database deployment!

In Case 2. you have to directly provide a valid JWT token in the
`Authorization` header, for some user which is configured in the
database deployment. You can acquire such a token by using the

```
POST $DATABASE_URL/_open/auth
```

API with a body like this:

```json
{"username":"root", "password": "asdazgqwegqzwe"}
```

Use the resulting token in the following HTTP header:

```
Authorization: bearer $TOKEN
```

